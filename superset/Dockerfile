# Use Python 3.8 base image
FROM python:3.8

ENV PYTHONUNBUFFERED=1

# Install system build tools and venv support
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /superset

# Create a virtual environment
RUN python3 -m venv /venv

# Upgrade pip and install Poetry in the venv
RUN /venv/bin/pip install --upgrade pip && /venv/bin/pip install poetry

# Copy the shared pyproject.toml
COPY pyproject.toml ./

# Instruct Poetry to use the existing venv
ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_VIRTUALENVS_IN_PROJECT=false

# Install dependencies
RUN /venv/bin/poetry install --no-root

# Copy the Superset configuration file and set its path
COPY superset/superset_config.py /superset_config.py
ENV SUPERSET_CONFIG_PATH=/superset_config.py

# Expose the Superset port
EXPOSE 8088

# Start Superset using gunicorn via Poetry
CMD ["/venv/bin/poetry", "run", "gunicorn", "-b", "0.0.0.0:8088", "superset.app:create_app()"]
