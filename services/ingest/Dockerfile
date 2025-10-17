FROM python:3.10-slim
WORKDIR /app
COPY libs/common /app/libs/common
ENV PYTHONPATH=/app/libs:${PYTHONPATH}
RUN pip install --no-cache-dir websockets httpx orjson python-dotenv asyncpg beautifulsoup4
RUN pip install --no-cache-dir -e /app/libs/common
COPY services/ingest/ingest /app/ingest
ENV PYTHONUNBUFFERED=1
CMD ["python","-m","ingest.run"]
