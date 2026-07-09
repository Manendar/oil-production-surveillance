FROM python:3.13-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY scripts/ ./scripts/
COPY alerting/ ./alerting/
COPY config.yml .

# Default command (can be overridden)
CMD ["python", "scripts/load/influx_writer.py"]
