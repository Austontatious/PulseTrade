FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev git curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

RUN pip install --no-cache-dir \
    pandas==2.2.* psycopg2-binary==2.9.* requests==2.32.* pyarrow==16.* \
    torch>=2.2 torchmetrics>=1.3 pytorch-lightning>=2.3 neuralforecast==1.7.4

CMD ["/bin/bash"]
