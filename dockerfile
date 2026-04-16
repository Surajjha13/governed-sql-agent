# syntax=docker/dockerfile:1.7

FROM python:3.10-slim-bookworm

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV IS_DOCKER=true
ENV TRANSFORMERS_CACHE=/app/models
ENV SENTENCE_TRANSFORMERS_HOME=/app/models

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only requirements first (for caching)
COPY requirements.txt .

# Install dependencies using Docker BuildKit cache mount for pip
# This significantly speeds up rebuilt layers if requirements haven't changed
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.txt

# Pre-download ML models used for semantic search
# This ensures the container starts instantly and works offline
COPY download_models.py .
RUN mkdir -p /app/models /app/logs /app/tmp \
    && python download_models.py

# Copy the rest of the code
COPY . .

# Run as a non-root user in production using fixed IDs for reliability across Docker environments.
RUN groupadd --system --gid 10001 appgroup \
    && useradd --system --uid 10001 --gid 10001 --create-home --home-dir /home/appuser appuser \
    && chown -R 10001:10001 /app /home/appuser

USER 10001:10001

EXPOSE 8000

# Start the app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
