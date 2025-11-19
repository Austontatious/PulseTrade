FROM python:3.10-slim
WORKDIR /app
COPY libs/common /app/libs/common
ENV PYTHONPATH=/app/libs:${PYTHONPATH}
RUN pip install --no-cache-dir celery[redis] orjson asyncpg numpy pandas vaderSentiment httpx
RUN pip install --no-cache-dir -e /app/libs/common
RUN mkdir -p /app/services/ingest
COPY services/ingest/ingest/config.py /app/services/ingest/config.py
COPY services/worker/worker /app/worker
ENV PYTHONUNBUFFERED=1
CMD ["python","-m","worker.runner"]
