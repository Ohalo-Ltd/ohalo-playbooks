FROM python:3.13.3-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install the Python dependencies from uv.lock
ENV UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project

# Copy source code
COPY src/ ./src/
RUN uv sync --frozen
ENV PATH="/app/.venv/bin:$PATH"

# Expose SMTP proxy port
EXPOSE 1025

# Run the SMTP proxy
CMD ["python", "-m", "src.smtp_proxy.server"]
