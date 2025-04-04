# Use Python 3.8 base image
FROM python:3.8

ENV PYTHONUNBUFFERED=1

# Install venv support
RUN apt-get update && apt-get install -y python3-venv && rm -rf /var/lib/apt/lists/*

WORKDIR /app/dagster_code

# Create a virtual environment
RUN python3 -m venv /venv

# Upgrade pip and install Poetry inside the venv
RUN /venv/bin/pip install --upgrade pip && /venv/bin/pip install poetry

# Copy the shared pyproject.toml
COPY pyproject.toml ./

# Configure Poetry to use the existing venv
ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_VIRTUALENVS_IN_PROJECT=false

# Install project dependencies
RUN /venv/bin/poetry install --no-root

# Copy the Dagster pipeline code
COPY dagster_project/pipeline.py .
COPY dagster_project/dagster.yaml .

# Set Dagster's home directory and expose its port
ENV DAGSTER_HOME=/dagster_home
EXPOSE 3000

# Run dagit via Poetry
CMD ["/venv/bin/poetry", "run", "dagit", "-f", "/app/dagster_code/pipeline.py", "-a", "defs", "-h", "0.0.0.0", "-p", "3000"]
