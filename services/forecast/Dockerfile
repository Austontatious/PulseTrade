FROM python:3.10-slim
WORKDIR /app
COPY libs/common /app/libs/common
ENV PYTHONPATH=/app/libs:${PYTHONPATH}
RUN pip install --no-cache-dir asyncpg numpy pandas orjson
# Optional: TimesFM/Chronos later
# RUN pip install timesfm amazon-chronos
RUN pip install --no-cache-dir -e /app/libs/common
COPY services/forecast/forecast /app/forecast
ENV PYTHONUNBUFFERED=1
CMD ["python","-m","forecast.run"]
