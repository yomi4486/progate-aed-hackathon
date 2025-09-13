FROM python:3.12-slim AS base

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV UV_CACHE_DIR=/tmp/.uv-cache

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Create app user
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Copy dependency files and README (required for package building)
COPY pyproject.toml uv.lock README.md ./

# Create virtual environment and install dependencies
RUN uv sync --frozen --no-dev

# Copy application code
COPY app/ ./app/

# Ensure proper permissions
RUN chown -R appuser:appuser /app
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -m app.crawler.worker health || exit 1

# Default command (use virtual environment)
CMD ["./.venv/bin/python", "-m", "app.crawler.worker", "run"]

FROM base AS production

# Override for production environment
ENV CRAWLER_ENV=prod
ENV LOG_LEVEL=INFO

# Use production configuration by default (use virtual environment)
CMD ["./.venv/bin/python", "-m", "app.crawler.worker", "run", "--environment", "prod", "--log-level", "INFO"]

FROM base AS development

# Install development dependencies
USER root
RUN uv sync --frozen
USER appuser

ENV CRAWLER_ENV=devlocal
ENV LOG_LEVEL=DEBUG

CMD ["./.venv/bin/python", "-m", "app.crawler.worker", "run", "--environment", "devlocal", "--log-level", "DEBUG"]
