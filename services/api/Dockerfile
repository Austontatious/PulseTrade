FROM python:3.10-slim
WORKDIR /app
COPY libs/common /app/libs/common
COPY libs/llm /app/libs/llm
COPY configs /app/configs
ENV PYTHONPATH=/app/libs:${PYTHONPATH}
RUN pip install --no-cache-dir fastapi uvicorn[standard] pydantic-settings prometheus-client asyncpg==0.29.0 sqlalchemy[asyncio] orjson pyyaml psycopg2-binary
RUN pip install --no-cache-dir -e /app/libs/common
COPY services/api/app /app/app
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
