# Multi-stage production Dockerfile for Event-Driven Telemetry Mesh
FROM python:3.12-slim AS base

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application source code and packaging manifests
COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY telemetry_mesh/ ./telemetry_mesh/
COPY scripts/ ./scripts/
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Install package in editable mode
RUN pip install --no-cache-dir -e .

# Create unprivileged application user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000 9191

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH="/app:/app/src"

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Default command starts API server & dashboard
CMD ["uvicorn", "telemetry_mesh.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
