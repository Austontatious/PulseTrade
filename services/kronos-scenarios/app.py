from fastapi import Depends, FastAPI, HTTPException, Header
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
import os, json
import torch, numpy as np

API_KEY = os.getenv("API_KEY")
MODEL_DIR = os.getenv("SCENARIO_MODEL_DIR", "/models/kronos-scenarios/latest")

app = FastAPI(title="kronos-scenarios", version="0.1.0")


def require_key(x_api_key: str | None = Header(default=None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "bad api key")


_model = None
_cfg = {}


def load_model():
    global _model, _cfg
    with open(os.path.join(MODEL_DIR, "config.json")) as f:
        _cfg = json.load(f)
    ckpt = torch.load(os.path.join(MODEL_DIR, "state.pt"), map_location="cpu")
    from libs.kronos_scenarios.student_diffusion import StudentDiffusionDecoder

    _model = StudentDiffusionDecoder(**ckpt["hparams"])
    _model.load_state_dict(ckpt["state_dict"])
    _model.eval()


@app.on_event("startup")
def _startup():
    load_model()


class SampleReq(BaseModel):
    symbols: List[str] = Field(..., description="Universe to simulate")
    context_days: int = 60
    forecast_horizon: int = 20
    num_paths: int = 2000
    conditioning: Optional[Dict] = Field(
        default=None, description="Optional conditioning features (e.g., news_count, vix)"
    )


class SampleResp(BaseModel):
    symbols: List[str]
    horizon: int
    num_paths: int
    pctl: Dict[str, List[float]]
    es: Dict[str, float]
    meta: Dict[str, str]


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/sample", dependencies=[Depends(require_key)], response_model=SampleResp)
def sample(req: SampleReq):
    if req.num_paths < 100 or req.num_paths > 20000:
        raise HTTPException(400, "num_paths out of bounds [100, 20000]")
    with torch.no_grad():
        paths = _model.sample(
            num_symbols=len(req.symbols),
            context_len=req.context_days,
            horizon=req.forecast_horizon,
            num_paths=req.num_paths,
            conditioning=req.conditioning or {},
        )
        ret = paths.mean(dim=2).cpu().numpy()
        pctls = [1, 5, 50, 95, 99]
        pctl = {f"p{p}": np.percentile(ret, p, axis=0).tolist() for p in pctls}
        es = {"es95": float(ret[ret <= np.percentile(ret, 5)].mean()) if ret.size else 0.0}
    return SampleResp(
        symbols=req.symbols,
        horizon=req.forecast_horizon,
        num_paths=req.num_paths,
        pctl=pctl,
        es=es,
        meta={"model": "student-diffusion", "ver": _cfg.get("version", "unknown")},
    )
