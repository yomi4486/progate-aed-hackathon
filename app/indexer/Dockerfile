# Multi-stage build for optimized indexer service image
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim as builder

# Set working directory
WORKDIR /app

# Copy uv configuration files
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev --no-install-project

# Production stage
FROM python:3.12-slim-bookworm

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r indexer && useradd -r -g indexer indexer

# Copy virtual environment from builder stage
COPY --from=builder --chown=indexer:indexer /app/.venv /app/.venv

# Set environment variables
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app:$PYTHONPATH"
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy application code
COPY --chown=indexer:indexer . .

# Switch to non-root user
USER indexer

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "from app.indexer.cli import health_check; import asyncio; from app.indexer.config import IndexerConfig; exit(0 if asyncio.run(health_check(IndexerConfig.from_environment())) else 1)" || exit 1

# Default command
CMD ["indexer", "run"]