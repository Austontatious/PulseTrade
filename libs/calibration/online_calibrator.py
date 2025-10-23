from __future__ import annotations

import math
import time
from dataclasses import dataclass, asdict
from typing import Dict, Optional


@dataclass
class CalibState:
    bias: float = 0.0
    var: float = 1e-6
    cov: float = 0.90
    scale: float = 1.0
    n: int = 0
    updated_ts: float = 0.0


class OnlineCalibrator:
    def __init__(
        self,
        lam: float = 0.02,
        target_cov: float = 0.90,
        max_bias: float = 0.003,
        min_scale: float = 0.8,
        max_scale: float = 1.8,
        scale_kappa: float = 0.05,
        outlier_sigma: float = 4.0,
        min_obs: int = 20,
    ):
        self.lam = lam
        self.target_cov = target_cov
        self.max_bias = max_bias
        self.min_scale = min_scale
        self.max_scale = max_scale
        self.scale_kappa = scale_kappa
        self.outlier_sigma = outlier_sigma
        self.min_obs = min_obs
        self.states: Dict[str, CalibState] = {}
        self.sector_map: Dict[str, str] = {}
        self.sector_states: Dict[str, CalibState] = {}
        self.global_state = CalibState()

    def _get(self, key: str, level: str = "symbol") -> CalibState:
        store = self.states if level == "symbol" else self.sector_states
        if key not in store:
            store[key] = CalibState(updated_ts=time.time())
        return store[key]

    def update_symbol(
        self,
        symbol: str,
        residual: float,
        inside_interval: bool,
        skip: bool = False,
        sector: Optional[str] = None,
    ) -> None:
        if skip:
            return
        st = self._get(symbol, "symbol")
        sigma = math.sqrt(max(st.var, 1e-8))
        if st.n >= self.min_obs and abs(residual) > self.outlier_sigma * sigma:
            return
        lam = self.lam
        st.bias = (1 - lam) * st.bias + lam * residual
        st.var = (1 - lam) * st.var + lam * (residual * residual)
        st.cov = (1 - lam) * st.cov + lam * (1.0 if inside_interval else 0.0)
        st.n += 1
        st.updated_ts = time.time()
        cov_err = self.target_cov - st.cov
        st.scale = max(self.min_scale, min(self.max_scale, st.scale * (1 + self.scale_kappa * cov_err)))
        st.bias = max(-self.max_bias, min(self.max_bias, st.bias))
        if sector:
            sst = self._get(sector, "sector")
            sst.bias = (1 - lam) * sst.bias + lam * residual
            sst.var = (1 - lam) * sst.var + lam * (residual * residual)
            sst.cov = (1 - lam) * sst.cov + lam * (1.0 if inside_interval else 0.0)
            sst.scale = max(
                self.min_scale, min(self.max_scale, sst.scale * (1 + 0.5 * self.scale_kappa * cov_err))
            )
            sst.n += 1
            sst.updated_ts = time.time()
        gst = self.global_state
        gst.bias = (1 - lam) * gst.bias + lam * residual
        gst.var = (1 - lam) * gst.var + lam * (residual * residual)
        gst.cov = (1 - lam) * gst.cov + lam * (1.0 if inside_interval else 0.0)
        gst.scale = max(
            self.min_scale, min(self.max_scale, gst.scale * (1 + 0.25 * self.scale_kappa * cov_err))
        )
        gst.n += 1
        gst.updated_ts = time.time()

    def get_effective(self, symbol: str, sector: Optional[str] = None) -> CalibState:
        s = self.states.get(symbol, CalibState())
        if sector:
            t = self.sector_states.get(sector, CalibState())
        else:
            t = CalibState()
        g = self.global_state
        ns = max(s.n, 0)
        nt = max(t.n, 0)
        ng = max(g.n, 1)
        ws = min(1.0, ns / (ns + 20))
        wt = (1 - ws) * (nt / (nt + 50)) if nt > 0 else 0.0
        wg = 1 - ws - wt
        blend = CalibState(
            bias=ws * s.bias + wt * t.bias + wg * g.bias,
            var=ws * s.var + wt * t.var + wg * g.var,
            cov=ws * s.cov + wt * t.cov + wg * g.cov,
            scale=ws * s.scale + wt * t.scale + wg * g.scale,
            n=ns,
            updated_ts=max(s.updated_ts, t.updated_ts, g.updated_ts),
        )
        blend.bias = max(-self.max_bias, min(self.max_bias, blend.bias))
        blend.scale = max(self.min_scale, min(self.max_scale, blend.scale))
        return blend

    def to_records(self):
        out = []
        for key, state in self.states.items():
            rec = asdict(state)
            rec["key"] = key
            rec["level"] = "symbol"
            out.append(rec)
        for key, state in self.sector_states.items():
            rec = asdict(state)
            rec["key"] = key
            rec["level"] = "sector"
            out.append(rec)
        g = asdict(self.global_state)
        g["key"] = "GLOBAL"
        g["level"] = "global"
        out.append(g)
        return out
