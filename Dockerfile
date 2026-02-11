FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv and Python dependencies
RUN pip3 install --upgrade pip && \
    pip3 install uv
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --inexact --no-dev

COPY src/ src/

EXPOSE 15002

ENV UV_PROJECT_ENVIRONMENT=/usr/local
CMD ["uv", "run", "python", "-m", "spark_connect_proxy"]
