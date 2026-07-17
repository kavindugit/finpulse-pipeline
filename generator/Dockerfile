FROM python:3.11-slim



WORKDIR /app

# Copy and install dependencies first (Docker layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# PaySim seed is mounted at runtime via docker-compose volume → /data/
# config.py checks PAYSIM_PARQUET_PATH env var for the exact path.
ENV PAYSIM_PARQUET_PATH=/data/paysim_seed.parquet

# Default: connect to Kafka on the Docker-internal listener.
# Override with --broker or KAFKA_BROKER env var.
CMD ["python", "producer.py", "--broker", "kafka:29092"]
