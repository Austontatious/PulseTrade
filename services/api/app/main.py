from fastapi import FastAPI, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from .routes import health, signals, analytics, strategist, llm, llm_admin

app = FastAPI(title="PulseTrade API", version="0.1.0")

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

app.include_router(health.router)
app.include_router(signals.router, prefix="/signals")
app.include_router(analytics.router, prefix="/analytics")
app.include_router(strategist.router, prefix="/strategist")
app.include_router(llm.router)
app.include_router(llm_admin.router)
