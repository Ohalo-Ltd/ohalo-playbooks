FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PATH="/root/.local/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl build-essential git && \
    rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /app

COPY pyproject.toml README.md ./

# Install dependencies into a project-local virtual environment
RUN uv venv && uv sync

# Copy the remainder of the application code
COPY . .

ENTRYPOINT ["uv", "run", "main.py"]
