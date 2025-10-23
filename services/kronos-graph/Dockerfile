FROM python:3.11-slim

ENV TMPDIR=/var/tmp
RUN mkdir -p /var/tmp && chmod 1777 /var/tmp
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app.py .
EXPOSE 8081
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8081"]
