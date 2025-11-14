FROM python:3.10-slim
WORKDIR /app
COPY libs/common /app/libs/common
COPY libs/kronos_client /app/libs/kronos_client
COPY libs/llm /app/libs/llm
COPY configs /app/configs
ENV PYTHONPATH=/app/libs:${PYTHONPATH}
RUN pip install --no-cache-dir asyncpg==0.29.0 numpy pandas orjson httpx pyyaml psycopg2-binary prometheus-client
# Optional: TimesFM/Chronos later
# RUN pip install timesfm amazon-chronos
RUN pip install --no-cache-dir -e /app/libs/common
COPY services/forecast/forecast /app/forecast
ENV PYTHONUNBUFFERED=1
CMD ["python","-m","forecast.run"]
