FROM python:3.13.3-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY pyproject.toml .
COPY README.md .
RUN pip install -e .

# Copy source code
COPY src/ ./src/

# Expose SMTP proxy port
EXPOSE 1025

# Run the SMTP proxy
CMD ["python", "-m", "src.smtp_proxy.server"]
