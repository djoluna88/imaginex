# NBA players pipeline: one entry point for the local workflow.
#
# Works the same on your machine and inside the Docker image. Every target below is
# documented with a trailing "## comment"; `make help` lists them.
#
#   make install        local virtualenv with test dependencies (host only)
#   make test           unit + oracle tests on a local Spark (needs Java 17+, or use docker-test)
#   make simulate       regenerate the simulated snapshot from the seed
#                       (chain a later one: make simulate VERSION=NBA2k24 SOURCE=data/raw/nba2k23-simulated.csv)
#   make deploy         upload the bundle to the workspace in .env
#   make run            run the job for one source file
#   make run-all        run the job for the initial file, then for every *-simulated.csv in name order
#
# Databricks credentials come from .env (see .env.example). The file is read here so the
# CLI targets work on the host; inside Docker, compose injects the same variables.

-include .env
export DATABRICKS_HOST DATABRICKS_TOKEN

# --------------------------------------------------------------------------- #
# Knobs. Override on the command line: make run SOURCE_FILE=nba2k23-simulated.csv
# --------------------------------------------------------------------------- #

TARGET      ?= dev                      # bundle target in databricks.yml
JOB         ?= nba_pipeline             # resource key in resources/job.yml
SOURCE_FILE ?= nba2k-full.csv           # which CSV the job processes
ON_FAILURE  ?= quarantine               # quarantine | fail
VERSION     ?= NBA2k23                  # simulated snapshot version (must be in SNAPSHOT_DATES)
SEED        ?= 20220909                 # simulator RNG seed
SOURCE      ?= data/raw/nba2k-full.csv  # snapshot the simulator starts from; use a *-simulated.csv to chain
PYTEST_ARGS ?=                          # e.g. PYTEST_ARGS="tests/test_gold.py -k rerun"

# Python: the local .venv when it exists, otherwise whatever python3 is on PATH.
# compose.yaml sets PYTHON=python so the container never picks up a macOS .venv.
PYTHON      ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

# make strips comments but keeps trailing spaces on ?= lines; trim them.
TARGET      := $(strip $(TARGET))
JOB         := $(strip $(JOB))
SOURCE_FILE := $(strip $(SOURCE_FILE))
ON_FAILURE  := $(strip $(ON_FAILURE))
VERSION     := $(strip $(VERSION))
SEED        := $(strip $(SEED))
SOURCE      := $(strip $(SOURCE))

# Every simulated snapshot committed in data/raw, in name order (nba2k23 < nba2k24 ...).
SIMULATED_FILES := $(notdir $(sort $(wildcard data/raw/*-simulated.csv)))

.DEFAULT_GOAL := help
.PHONY: help install test simulate validate plan deploy run run-all docker-build docker-test docker-shell clean

# --------------------------------------------------------------------------- #
# Local development                                                           #
# --------------------------------------------------------------------------- #

help: ## List the available targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## Create .venv (Python 3.11) with the project and its test dependencies
	python3.11 -m venv .venv || python3 -m venv .venv
	.venv/bin/pip install -q --upgrade pip
	.venv/bin/pip install -q -e ".[dev]"
	@echo "OK. The Spark tests also need a Java 17+ runtime (macOS: brew install openjdk@17), or use 'make docker-test'."

test: ## Run the test suite on a local Spark session
	$(PYTHON) -m pytest $(PYTEST_ARGS)

simulate: ## Generate data/raw/<version>-simulated.csv and its change log from SOURCE and SEED
	$(PYTHON) -m nba_pipeline.simulate --version $(VERSION) --seed $(SEED) --source $(SOURCE)

# --------------------------------------------------------------------------- #
# Databricks                                                                  #
# --------------------------------------------------------------------------- #

validate: ## Check databricks.yml and resources/*.yml without deploying
	databricks bundle validate -t $(TARGET)

plan: ## Check how many "Objects" were changed, updated or need to add as a first time
	databricks bundle plan -t $(TARGET)

deploy: ## Upload code, data/raw and the job definition to the workspace
	databricks bundle deploy -t $(TARGET)

run: ## Run the job for SOURCE_FILE (setup -> bronze -> silver -> gold)
	databricks bundle run $(JOB) -t $(TARGET) --params source_file=$(SOURCE_FILE),on_failure=$(ON_FAILURE)

run-all: ## Run the job for the initial snapshot, then for every simulated one in data/raw, in order
	$(MAKE) run SOURCE_FILE=nba2k-full.csv
	@for f in $(SIMULATED_FILES); do $(MAKE) run SOURCE_FILE=$$f || exit $$?; done

# --------------------------------------------------------------------------- #
# Docker: the same targets, inside the image (no local Python or Java needed)  #
# --------------------------------------------------------------------------- #

docker-build: ## Build the toolchain image (Python 3.11, JRE, Databricks CLI)
	docker compose build

docker-test: ## Run the test suite inside the container
	docker compose run --rm pipeline make test PYTEST_ARGS="$(PYTEST_ARGS)"

docker-shell: ## Open a shell inside the container with the repo mounted at /app
	docker compose run --rm pipeline bash

# --------------------------------------------------------------------------- #
# Housekeeping                                                                #
# --------------------------------------------------------------------------- #

clean: ## Remove Python caches, test caches and build metadata
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
	rm -rf .pytest_cache src/*.egg-info