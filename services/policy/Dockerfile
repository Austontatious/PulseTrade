FROM python:3.10-slim
WORKDIR /app
COPY libs/common /app/libs/common
COPY libs/portfolio /app/libs/portfolio
COPY libs/broker /app/libs/broker
ENV PYTHONPATH=/app/libs:${PYTHONPATH}
RUN pip install --no-cache-dir asyncpg orjson numpy httpx
RUN pip install --no-cache-dir -e /app/libs/common
COPY services/policy/policy /app/policy
ENV PYTHONUNBUFFERED=1
CMD ["python","-m","policy.run"]
