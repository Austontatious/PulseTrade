# LLM Runbook (“Lexi” shared instance)

PulseTrade now connects to the **Lexi** vLLM server that runs alongside (but outside) the Docker Compose stack. No `llm` container is launched by Compose; instead, all callers hit an OpenAI-compatible endpoint exposed on the host at `http://host.docker.internal:8008/v1`.

## Overview

- Host process (example launch command):

  ```bash
  export CUDA_VISIBLE_DEVICES=0,1,2,3
  /mnt/data/vllm-venv/bin/python -m vllm.entrypoints.openai.api_server \
    --model /mnt/data/models/Qwen/Qwen2.5-32B-AGI \
    --served-model-name Lexi \
    --tensor-parallel-size 4 \
    --dtype float16 \
    --max-model-len 4096 \
    --gpu-memory-utilization 0.88 \
    --max-num-seqs 64 \
    --swap-space 12 \
    --distributed-executor-backend mp \
    --trust-remote-code \
    --download-dir /mnt/data/models \
    --disable-custom-all-reduce \
    --host 0.0.0.0 --port 8008
  ```

- Consumers inside Compose:
  - Forecast service (adds `features.llm` payloads to each stored forecast row).
  - API `/llm` routes (health + ad-hoc chat).
  - Analytics `/analytics/recent`.
  - Admin `/llm/admin/*` and strategist tooling.
- Networking: `docker-compose.yml` injects `extra_hosts: ["host.docker.internal:host-gateway"]` for API/forecast so they can reach port `8008` on the host.

## Configuration

Environment knobs (see `.env` / `.env.example`):

| Variable | Description |
| --- | --- |
| `LLM_BASE_URL` | Base URL used by core services (`http://host.docker.internal:8008/v1`) |
| `LLM_MODEL` | Model name advertised by Lexi (e.g., `Lexi`) |
| `LLM_DTYPE` | Precision (`float16`, `bfloat16`, etc.) |
| `LLM_MAX_TOKENS` | Context window passed to vLLM (`--max-model-len`) |
| `LLM_GPU_UTILIZATION` | Fractional GPU memory reservation |
| `LLM_TEMPERATURE`, `LLM_TOP_P`, `LLM_MAX_OUTPUT_TOKENS` | Defaults for the chat helper |
| `LLM_SHADOW_MODE` | When true, policy decisions are logged but do not gate live orders |
| `LLM_AB_BUCKETS` | Optional A/B split (`rationale:1.0.0,1.1.0;policy:1.0.0`) |
| `ORDER_GATE_DISABLED` | Emergency override to treat policy checks as allow |

### Prompt registry

Prompts are versioned in `configs/llm_prompts.yaml`. Use the helper in `libs/llm/registry.py` to fetch the active version or compute hashes. Bump the `version` field when editing templates and roll out via `LLM_AB_BUCKETS` before promoting globally.

## Health checks

- Through API proxy: `curl -s http://localhost:8001/llm/health` (returns cached flag + `ok` if Lexi replies with “OK”)
- Direct host endpoint: `curl -s http://localhost:8008/v1/models`

## Troubleshooting

| Symptom | Action |
| --- | --- |
| API health check returns `Connection refused` | Confirm the host Lexi process is running and listening on `0.0.0.0:8008`. |
| API health returns `{ok:false}` but Lexi is up | Lexi’s response must be the literal string `OK` for the proxy to mark it healthy; adjust the health prompt or response template. |
| CUDA OOM inside Lexi | Lower `--max-model-len`, `--gpu-memory-utilization`, or reduce tensor-parallel size; restart the host process. |
| Policy filter rejects every trade | Inspect `features->'llm'->'policy'->'raw'` in the `forecasts` table for the raw LLM response |
| Summaries missing | `/analytics/recent` returns `summary=""` when the LLM is unreachable or times out |
| Replay returns cached response | Call `/llm/admin/replay` with `{"id": ..., "force": true}` to bypass the cache |
| Need immediate flat | Enable `ENABLE_PANIC_EXIT=1` and POST `/strategist/panic-exit` |

## Testing

```bash
make test-llm
```

The tests issue a real completion. They skip automatically if `LLM_BASE_URL` cannot be reached.

## Data retention

The forecast service persists LLM outputs inside the `features` JSON on the `forecasts` table, grouped under the `llm` key (`rationale`, `policy`, and diagnostic inputs). No additional state lives outside Postgres or the mounted model directory.

### Offline evaluation

`tools/llm_eval/run_eval.py` replays historical signals, records JSON validity and latency, and stores results under `results/`. Trigger via `make eval-llm` inside the compose stack (uses the `tools` container).
