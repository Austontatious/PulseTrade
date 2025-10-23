from fastapi import Depends, FastAPI, HTTPException, Header
from pydantic import BaseModel, Field
from typing import List, Dict
import os
import json
import torch
import numpy as np

API_KEY = os.getenv("API_KEY")
MODEL_DIR = os.getenv("GRAPHX_MODEL_DIR", "/models/kronos-graphx/latest")

app = FastAPI(title="kronos-graphx", version="0.1.0")


def require_key(x_api_key: str | None = Header(default=None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "bad api key")


_model = None
_syms = None
_cfg = {}


def load_model():
    global _model, _cfg, _syms
    with open(os.path.join(MODEL_DIR, "config.json"), "r", encoding="utf-8") as f:
        _cfg = json.load(f)
    ckpt = torch.load(os.path.join(MODEL_DIR, "state.pt"), map_location="cpu")
    from libs.kronos_graphx.graph_transformer import GraphAttentionModel

    _model = GraphAttentionModel(**ckpt["hparams"])
    _model.load_state_dict(ckpt["state_dict"])
    _model.eval()
    _syms = _cfg["symbols"]


@app.on_event("startup")
def _startup():
    load_model()


class WeightsReq(BaseModel):
    window: List[List[float]] = Field(
        ..., description="[T x S] recent returns aligned to training order"
    )


class WeightsResp(BaseModel):
    symbols: List[str]
    attn: List[List[float]]
    leaders: List[str]
    meta: Dict[str, str]


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/graph_weights", dependencies=[Depends(require_key)], response_model=WeightsResp)
def graph_weights(req: WeightsReq):
    W = np.array(req.window, dtype=np.float32)
    if W.ndim != 2 or W.shape[1] != len(_syms):
        raise HTTPException(400, "window shape must be [T, S] with S == len(symbols)")
    with torch.no_grad():
        attn = _model.context_attention(torch.from_numpy(W).unsqueeze(0))
        A = attn[0].cpu().numpy()
        v = np.ones((A.shape[0],), dtype=np.float32) / A.shape[0]
        for _ in range(20):
            v = A @ v
            norm = np.linalg.norm(v)
            if norm == 0:
                break
            v = v / norm
        order = np.argsort(-v)[: min(10, len(v))]
        leaders = [_syms[i] for i in order]
    return WeightsResp(
        symbols=_syms,
        attn=A.tolist(),
        leaders=leaders,
        meta={"model": "graphx-transformer", "ver": _cfg.get("version", "unknown")},
    )
