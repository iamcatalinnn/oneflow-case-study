FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/app/containerization

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

COPY data /app/data
COPY ingestion /app/ingestion
COPY main /app/main
COPY containerization /app/containerization

RUN chmod +x /app/containerization/run_pipeline.sh

CMD ["/app/containerization/run_pipeline.sh"]