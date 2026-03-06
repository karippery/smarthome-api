FROM python:3.11-slim-bookworm

WORKDIR /app

# Non-root user (security!)
RUN useradd --create-home --shell /bin/bash app
USER app
ENV HOME=/home/app
WORKDIR $HOME/app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/home/app/.venv \
    PATH="/home/app/.venv/bin:$PATH"

# Install system deps (as root, then switch back)
USER root
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*
USER app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.8.14 /uv /uvx /bin/

# Copy lock + project files
COPY --chown=app:app pyproject.toml uv.lock ./

# Install **only production deps** (--no-dev)
RUN uv sync --frozen --no-dev

# Copy code (immutable!)
COPY --chown=app:app . .


# No entrypoint.sh in prod (handle deps, migrations via CI/CD or init containers in K8s)
EXPOSE 8000

# Use Gunicorn
CMD ["uv", "run", "gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "2"]
