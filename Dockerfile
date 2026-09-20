# Local toolchain for the NBA players pipeline: Python, a JRE for local PySpark (tests), Databricks CLI, make.
# The pipeline itself runs on Databricks serverless; this image prepares data, runs the tests and drives the bundle.
FROM python:3.11-slim

# unzip: the Databricks CLI installer below needs it and the slim image does not ship it
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip make ca-certificates default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Databricks CLI (installs to /usr/local/bin/databricks)
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

WORKDIR /app

# Dependencies first, so code changes do not reinstall them. The editable install needs src/ to
# exist, so an empty placeholder package stands in for it; at runtime compose mounts the real
# repo over /app and the editable link (/app/src) resolves to your working copy.
COPY pyproject.toml ./
RUN mkdir -p src/nba_pipeline && touch src/nba_pipeline/__init__.py \
    && pip install --no-cache-dir -e ".[dev]"
# Not ".[dev,local]": databricks-connect replaces pyspark with a build that only allows remote
# sessions, which breaks the local Spark the tests use. The bundle needs the CLI, not Connect.

# The repo is mounted at runtime (see compose.yaml), so generated files land in your working copy
CMD ["make", "help"]