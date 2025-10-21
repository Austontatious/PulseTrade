FROM python:3.10-slim
WORKDIR /app
COPY libs/common /app/libs/common
ENV PYTHONPATH=/app/libs:${PYTHONPATH}
RUN pip install --no-cache-dir asyncpg orjson numpy pandas httpx beautifulsoup4
RUN pip install --no-cache-dir -e /app/libs/common
COPY services/strategist/strategist /app/strategist
ENV PYTHONUNBUFFERED=1
CMD ["python","-m","strategist.run"]

