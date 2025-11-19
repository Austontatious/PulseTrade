FROM python:3.10-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl dnsutils iputils-ping && \
    rm -rf /var/lib/apt/lists/*
COPY libs/common /app/libs/common
COPY libs/data /app/libs/data
RUN mkdir -p /app/services/ingest
COPY services/ingest/ingest/config.py /app/services/ingest/config.py
ENV PYTHONPATH=/app:/app/libs:${PYTHONPATH}
RUN pip install --no-cache-dir websockets httpx orjson python-dotenv asyncpg psycopg2-binary beautifulsoup4
RUN pip install --no-cache-dir -e /app/libs/common
COPY services/ingest/ingest /app/ingest
ENV PYTHONUNBUFFERED=1
CMD ["python","-m","ingest.run"]
