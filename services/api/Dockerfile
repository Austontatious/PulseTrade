FROM python:3.10-slim
WORKDIR /app
COPY libs/common /app/libs/common
ENV PYTHONPATH=/app/libs:${PYTHONPATH}
RUN pip install --no-cache-dir fastapi uvicorn[standard] pydantic-settings prometheus-client asyncpg sqlalchemy[asyncio] orjson
RUN pip install --no-cache-dir -e /app/libs/common
COPY services/api/app /app/app
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
