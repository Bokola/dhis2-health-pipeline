# DHIS2 Health Data Pipeline

A modular, production-oriented **Apache Spark health analytics pipeline** for ingesting, validating, transforming, and aggregating DHIS2 public health reporting data into analytics-ready storage layers.

The project follows a **Medallion Architecture pattern (Bronze → Silver → Gold)** to support scalable ETL processing, reproducible analytics, and downstream reporting workflows.

---

## Features

- Modular Spark task orchestration
- Bronze → Silver → Gold layered architecture
- Schema enforcement using strict validation rules
- Null-preserving health metric handling
- Analytical and stakeholder-specific output layers
- Dockerized execution environment
- `uv`-managed Python dependency workflow
- Automated test suite with isolated execution support
- CI/CD ready GitHub Actions integration

---

# Architecture Overview

The pipeline separates ingestion, transformation, analytics, and aggregation responsibilities into independently testable processing stages.

```text
                ┌─────────────────────────────────────┐
                │ Raw DHIS2 Extracts / Source Payloads│
                └─────────────────────────────────────┘
                                   │
                                   ▼
                          BRONZE LAYER (Raw)
                                   │
                                   ▼
                  ┌────────────────────────────────┐
                  │ Pipeline Ingestion Engine      │
                  │ - validation                   │
                  │ - cleaning                     │
                  │ - normalization                │
                  └────────────────────────────────┘
                                   │
                                   ▼
                  ┌────────────────────────────────┐
                  │ Transformation Engine          │
                  │ - schema enforcement           │
                  │ - type casting                 │
                  │ - business logic               │
                  └────────────────────────────────┘
                                   │
                                   ▼
                        SILVER LAYER (Curated)
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
         ┌───────────────────────┐   ┌────────────────────────┐
         │ task_06_analytics     │   │ task_07_aggregations   │
         │ time-series modeling  │   │ snapshot summaries     │
         └───────────────────────┘   └────────────────────────┘
                    │                             │
                    ▼                             ▼
        /output/analytics/            /output/aggregations/
             (.parquet)                     (.csv)

                        GOLD LAYER (Served Outputs)
```

---

# Data Processing Layers

## Bronze Layer — Raw Source Zone

Stores raw DHIS2 transactional extracts and unmodified payloads.

Characteristics:

- Source-preserving ingestion
- Minimal transformation
- Traceable audit lineage
- Supports replayable processing

Typical contents:

- Country reporting extracts
- Facility-level submissions
- DHIS2 API payload snapshots

---

## Silver Layer — Curated Warehouse Zone

Applies structured validation, schema enforcement, and transformation logic.

Key design principles:

- Strict Spark `StructType` enforcement
- Controlled type casting
- Business rule normalization
- Data quality validation

### Missing Data Strategy

Health reporting datasets frequently contain absent observations.

This pipeline intentionally preserves missing values as **true `NULL` states** instead of coercing them into zero values.

This prevents:

- artificial reporting inflation
- distorted averages
- biased completeness metrics
- misleading epidemiological interpretations

---

## Gold Layer — Analytics & Delivery Zone

Produces optimized analytical outputs for downstream consumption.

Two independent serving domains are maintained:

### Analytics Domain

Location:

```text
/output/analytics/
```

Format:

```text
.parquet
```

Use cases:

- rolling averages
- reporting rates
- month-over-month calculations
- longitudinal trend analysis
- BI dashboard consumption

---

### Aggregation Domain

Location:

```text
/output/aggregations/
```

Format:

```text
.csv
```

Use cases:

- quarterly summaries
- country completeness tables
- stakeholder exports
- matrix/pivot outputs

---

# Repository Structure

```text
.
├── data/
│   ├── data_values
│   ├── metadata
│   ├── org_units
│   └── programs
│
├── pipeline/
│   ├── ingestion/
│   ├── transformations/
│   ├── analytics/
│   ├── aggregations/
│   └── utils/
│
├── tests/
│   ├── integration/*
│   ├── unit/
│   └── fixtures/
│
├── output/
│   ├── aggregations/
│   ├── analytics/
│   ├── quarantine/
│   └── warehouse/
│
├── Dockerfile
├── pyproject.toml
├── uv.lock
└── README.md
```

---

# Technology Stack

| Component | Technology |
|------------|------------|
| Language | Python |
| Processing Engine | Apache Spark |
| Dependency Management | uv |
| Container Runtime | Docker |
| Testing | pytest |
| CI/CD | GitHub Actions |

---


---

### Task Consolidation & Engineering Design Choices (7 vs. 8 Modules)

While early design blueprints for this pipeline suggested an 8-task layout, the final production implementation consciously consolidates the execution workflow into **7 high-cohesion modules**. This was done to enforce clean separation of concerns and eliminate unnecessary intermediate disk I/O bottlenecks:

* **The Merger (Analytical Summaries & Matrix Aggregations):** Initial specifications separated the time-series windowing steps (Month-over-Month deltas, 3-month rolling averages) and the high-level reporting matrices (the pivoted data element coverage grid) into two distinct tasks. We merged these closely related operations into the final **Gold-tier processing modules (Task 06 and Task 07)**.
* **Elimination of Redundant Shared I/O Boundaries:** A strict 8-task breakdown would have forced the pipeline to write transient datasets to disk, only to immediately read them back into cluster memory. Combining these operations allows them to share an uninterrupted memory lineage.
* **Pipeline Performance Optimization:** Consolidating these steps allows Apache Spark's Catalyst Optimizer to build an efficient, unbroken Directed Acyclic Graph (DAG). This maximizes execution performance by pipelining transformations together in memory and eliminating redundant cross-node network shuffles.

The complete structural map of these 7 optimized execution blocks is fully detailed in the Pipeline Workflow Architecture diagram above.
---

# Installation

## Prerequisites

Required software:

- Python 3.11+
- Java 17
- Apache Spark compatible runtime
- Docker (optional)
- `uv`

Install `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

---

## Local Setup

Clone repository:

```bash
git clone dhis2-health-pipeline.git
cd dhis2-health-data-pipeline
```

Install dependencies:

```bash
uv sync
```

Activate environment:

```bash
source .venv/bin/activate
```

---

# Running the Pipeline

## Local Execution

Example:

```bash
uv run python pipeline/main.py
```

---

## Docker Execution

Build image:

```bash
docker build -t dhis2-pipeline .
```

Run container:

```bash
docker run --rm \
    -v $(pwd)/output:/app/output \
    dhis2-pipeline
```

---

# Ask-the-Data Assistant (LLM prototype)

Programme staff ask questions in plain language and get an answer computed from the gold layer, the SQL that produced it, and the caveats that apply.

```text
$ python -m assistant ask --show-sql "Which 5 Ugandan districts had the most RDT tests last quarter?"
```

### How it works

```text
question ──► Claude (tool use) ──► run_sql ──► SQL guard ──► locked DuckDB ──► rows back to Claude
                    │                                                (repeat to explore / fix errors)
                    └──► submit_answer(sql, answer, caveats) ──► final SQL re-run by us ──► + deterministic caveats
                    └──► decline(reason)   (data not in the warehouse: stock, population, targets ...)
```

| Layer | File | What it guarantees |
|---|---|---|
| Semantic layer | `assistant/catalog.py` | Six curated tables (`service_delivery`, `facilities`, `data_elements`, `country_reporting_rates`, `district_monthly_totals`, `quarantine_summary`) with plain-language column docs. Parquet is copied into in-memory DuckDB, then external access is disabled and the configuration locked, so no file can be read even if the guard were bypassed |
| SQL guard | `assistant/sql_guard.py` | Single `SELECT` only, allowlisted tables only, no file/settings/catalog functions, row cap. Errors go back to the model as fixable messages |
| Agent | `assistant/agent.py` | Tool-use loop with a step limit (last step forces a decision). The final SQL is re-executed rather than trusted. Caveats for recent months still filling in, NULL-is-not-zero and truncation are added in code, independent of the model |
| Evaluation | `assistant/eval/` | 32 questions: 26 with gold SQL scored by execution accuracy (row match, column names ignored), plus questions the data cannot answer and safety prompts that must be declined |

Domain conventions the model is told (and the gold answers follow): relative dates are relative to the latest data period, "last quarter" is the latest complete quarter, totals sum across category option combos, `PERCENTAGE` elements are averaged and never summed, facilities are grouped by uid because names repeat.

### Run it

```bash
export ANTHROPIC_API_KEY=...
uv run python generate_data.py && uv run python pipeline.py      # build ./output first
uv run python -m assistant ask "How many confirmed malaria cases did Uganda report last quarter?"
uv run python -m assistant chat --show-sql
uv run python -m assistant eval --repeats 3                       # writes output/assistant_eval/<timestamp>/report.md
```

The model defaults to `claude-sonnet-5-5`; override with `--model` or `ASSISTANT_MODEL` to compare models on the same question set.

### Open-weight models (no API key)

`assistant/llm_openai.py` lets the same agent, guard and eval run on any OpenAI-compatible server: Ollama, vLLM, llama.cpp `llama-server` or LM Studio. The model needs tool (function) calling support.

```bash
# ollama, on a laptop
ollama pull qwen2.5:14b            # ~9 GB; qwen2.5:7b if you have under 16 GB RAM
uv run python -m assistant --provider openai --model qwen2.5:14b ask "Which country had the lowest reporting rate last month?"
uv run python -m assistant --provider openai --model qwen2.5:14b eval

# vllm on a gpu box
vllm serve Qwen/Qwen2.5-14B-Instruct --enable-auto-tool-choice --tool-call-parser hermes
uv run python -m assistant --provider openai --base-url http://localhost:8000/v1 --model Qwen/Qwen2.5-14B-Instruct eval
```

Environment variables work too: `ASSISTANT_PROVIDER=openai`, `ASSISTANT_MODEL`, `OPENAI_BASE_URL`, and `OPENAI_API_KEY` for hosted OpenAI-compatible providers. Because the eval is identical across providers, the reports give a like-for-like comparison of hosted and open-weight models.

### Run it online with GitHub Actions

`.github/workflows/assistant-eval.yml` builds the gold layer on a runner, runs the eval, writes the report to the run summary and uploads `results.jsonl` as an artifact. Start it from **Actions → Ask-the-data assistant eval → Run workflow** and choose a backend:

| Backend | Setup | Cost | Notes |
|---|---|---|---|
| `github-models` (default) | none: the workflow's `GITHUB_TOKEN` with `models: read` | free, rate limited | `model: auto` picks a low-tier, tool-calling model from the live catalog (open-weight first). Low-tier daily quota covers about one full run per day; keep `repeats` at 1. Requests are throttled and results trimmed to fit the 8k-token input limit. If the run ends with a 403 (some orgs disable Models for Actions), add a fine-grained PAT with only *Models: read* as secret `MODELS_TOKEN` |
| `ollama` | none | free | Open-weight model running on the runner's CPU. Slow (expect an hour or more for `qwen2.5:7b`), but no quota, so it suits `repeats: 3`. Models are cached between runs |
| `anthropic` | repository secret `ANTHROPIC_API_KEY` | paid per token | Fastest and the reference result | The offline tests (`tests/test_assistant.py`) need neither an API key nor Spark.

---

# Testing

The project contains a dedicated automated testing suite under `/tests`.

Execute all tests:

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
uv run python -m pytest tests/ -v
```

Run specific categories:

### Unit Tests

```bash
uv run pytest tests/unit -v
```

### Integration Tests

```bash
uv run pytest tests/integration -v
```

---

# GitHub Actions CI Example

Example workflow:

`.github/workflows/pipeline.yml`

```yaml
name: Pipeline CI

on:
  push:
  pull_request:

jobs:
  test:

    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 17

      - uses: astral-sh/setup-uv@v5

      - name: install dependencies
        run: uv sync

      - name: run tests
        run: uv run pytest tests/ -v
```

---

# Design Principles

This project emphasizes:

- modular task isolation
- reproducible ETL execution
- strong schema contracts
- scalable analytical storage
- cloud/container portability
- automated validation pipelines

---

# Output Formats

| Format | Purpose |
|--------|---------|
| Parquet | High-performance analytical processing |
| CSV | Lightweight stakeholder distribution |

### Why Parquet?

Parquet enables:

- columnar compression
- predicate pushdown
- efficient BI scanning
- optimized analytical workloads

### Why CSV?

CSV supports:

- universal compatibility
- low-friction sharing
- spreadsheet interoperability
- lightweight reporting workflows

---

# Future Roadmap

Potential enhancements:

- Airflow orchestration
- Delta Lake support
- Incremental ingestion strategy
- Data quality dashboards
- Cloud storage integration
- Observability and monitoring layer

---

# Contributing

Contributions are welcome.

Recommended workflow:

1. Fork repository
2. Create feature branch
3. Commit changes
4. Run tests
5. Submit pull request

---

# License

Specify project license here.

Example:

```text
MIT License
```

---

# Maintainers

Project maintained by author.