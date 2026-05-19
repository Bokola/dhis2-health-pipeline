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