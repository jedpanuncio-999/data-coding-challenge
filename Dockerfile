FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY dagster_project/ dagster_project/

ENTRYPOINT ["dagster", "dev", "-m", "dagster_project.definitions", "-h", "0.0.0.0"]
