# DHIS2 Health Data Pipeline + Ask-the-Data LLM Assistant

An end-to-end health analytics project in two parts:

1. **A data pipeline.** A modular Apache Spark pipeline that turns raw DHIS2 health-reporting extracts into a validated, analytics-ready warehouse using a Bronze → Silver → Gold (medallion) design.
2. **An LLM prototype on top of it.** An *ask-the-data* assistant that lets non-technical programme staff ask questions in plain language ("Which Ugandan districts had the most RDT tests last quarter?") and get an answer computed from the warehouse, with the SQL that produced it and the caveats that apply.

The assistant is built to be **model-agnostic**: the same agent, guardrails and evaluation run on API-based platforms (Anthropic Claude, GitHub Models, any OpenAI-compatible service) and on open-source models running locally (Ollama, vLLM, llama.cpp, LM Studio). That makes it a working testbed for comparing hosted and open-weight models on a real health-data task.

---

## Highlights

- **LLM tool use, not free text.** The model works through three tools (`run_sql`, `submit_answer`, `decline`) and every number in an answer comes from a query the system re-executes itself.
- **Guardrails in code, not just in the prompt.** A read-only, locked-down DuckDB database, a SQL allowlist, and a semantic checker that validates every filter value against the real data.
- **Evaluation-driven development.** A 32-question benchmark with gold SQL, out-of-scope questions and prompt-injection tests, scored automatically and reported per category, with latency and token counts.
- **Open-source and API backends behind one interface.** Switch with a flag: `--provider anthropic` or `--provider openai --model qwen2.5:7b`.
- **Runs in CI.** A GitHub Actions workflow builds the warehouse and runs the evaluation on GitHub Models (free), Ollama on the runner (open weights), or Claude.
- **Production-minded data engineering.** Strict schemas, null-preserving metric handling, quarantine of unresolvable records, and a test suite that runs in CI.

---

## Quick start

```bash
git clone https://github.com/Bokola/dhis2-health-pipeline.git
cd dhis2-health-pipeline
uv sync

# 1. build the warehouse (needs Java 17)
uv run python generate_data.py
uv run python pipeline.py

# 2. ask a question with a free local open-source model
ollama pull qwen2.5:7b
uv run python -m assistant --provider openai --model qwen2.5:7b \
    ask --show-sql "Which country had the lowest reporting rate last month?"
```

Example output:

```text
Kenya had the lowest reporting rate last month.

country
Kenya

Caveats:
  - The most recent months (202510, 202511) may still be filling in: across the
    dataset 12.0% of values arrive more than 60 days after the month ends.

SQL:
SELECT country FROM country_reporting_rates WHERE period = '202511'
ORDER BY reporting_rate ASC LIMIT 1;
```

---

# Part 1: The data pipeline

## Architecture

```text
 Raw DHIS2 extracts (data values, metadata, org units, programs)
                           │
                           ▼
 BRONZE   task_01 ingestion ─ strict StructType schemas, raw preserved
                           │
                           ▼
 SILVER   task_02 metadata resolution ─ unresolvable data elements → quarantine
          task_03 org unit hierarchy  ─ orphaned facilities → quarantine
          task_04 quality flags       ─ type casting, missing / zero / late flags
                           │
                           ▼
 GOLD     task_05 dimensional model ─ star schema (fact_service_delivery + dims)
          task_06 analytics         ─ reporting rates, month-over-month, rolling averages (.parquet)
          task_07 aggregations      ─ quarterly summaries, coverage matrices (.csv)
                           │
                           ▼
          Ask-the-Data assistant (Part 2) reads the gold layer read-only
```

The workflow uses **7 high-cohesion tasks** rather than the 8 in early designs. The analytical windowing (month-over-month, rolling averages) and the reporting matrices were folded into tasks 06 and 07, so related transformations share one in-memory lineage. That avoids writing intermediate datasets to disk only to read them back, and lets Spark's Catalyst optimizer plan a single unbroken DAG.

## Processing layers

**Bronze (raw source zone).** Source-preserving ingestion with minimal transformation, so processing is traceable and replayable.

**Silver (curated zone).** Strict Spark `StructType` enforcement, controlled type casting, metadata and hierarchy resolution, and data-quality flags. Records that can't be resolved (unknown data elements, facilities missing from the hierarchy) are quarantined rather than silently dropped.

**Missing data strategy.** Health reporting data often has gaps. The pipeline keeps missing values as true `NULL` rather than coercing them to zero, which would inflate or deflate totals, distort averages and completeness metrics, and mislead epidemiological interpretation. Explicitly reported zeros are flagged separately (`is_explicit_zero`).

**Gold (analytics and delivery zone).**

| Domain | Location | Format | Used for |
|---|---|---|---|
| Warehouse | `output/warehouse/` | Parquet | Star schema: `fact_service_delivery`, `dim_org_unit`, `dim_data_element`, `dim_period`, `dim_program` |
| Analytics | `output/analytics/` | Parquet | Reporting rates, month-over-month change, rolling averages, BI dashboards |
| Aggregations | `output/aggregations/` | CSV | Quarterly summaries, completeness tables, stakeholder exports |
| Quarantine | `output/quarantine/` | Parquet | Rejected records, kept for audit |

Parquet gives columnar compression and fast analytical scans; CSV keeps stakeholder outputs easy to open in a spreadsheet.

## Running the pipeline

```bash
uv run python generate_data.py                                  # synthetic DHIS2-style data (seeded, reproducible)
uv run python pipeline.py --data-dir ./data --output-dir ./output
```

With Docker:

```bash
docker build -t dhis2-pipeline .
docker run --rm -v $(pwd)/output:/app/output dhis2-pipeline
```

> **Data-quality fix found while building the assistant.** The first assistant queries returned empty totals. Tracing that back showed task 04 only cast `NUMBER`/`INTEGER` value types, while DHIS2 sends `INTEGER_ZERO_OR_POSITIVE` and `PERCENTAGE`, so `cast_value` was NULL in every fact row and every value-based gold output was empty. The cast now covers all DHIS2 numeric types. Building an LLM interface on top of a pipeline turned out to be an effective end-to-end test of the pipeline itself.

---

# Part 2: Ask-the-Data assistant (LLM prototype)

## What it does

Programme staff ask a question in plain language. The assistant writes SQL against a curated view of the gold layer, runs it, checks the result, and answers with the numbers, the SQL it used and any caveats. If the data cannot answer the question, for example stock-outs, population denominators or targets, which are not in this warehouse, it says so instead of approximating.

## How it works

```text
question ──► LLM (tool calling) ──► run_sql ──► SQL guard ──► semantic check ──► locked DuckDB
                  │      ▲                                                        │
                  │      └──────────── rows, or a fixable error message ◄─────────┘
                  │
                  ├──► submit_answer(sql, answer, caveats) ──► review ──► final SQL re-run by the system
                  │                                                    + caveats added in code
                  └──► decline(reason)
```

| Layer | File | What it guarantees |
|---|---|---|
| Semantic layer | `assistant/catalog.py` | Six curated tables (`service_delivery`, `facilities`, `data_elements`, `country_reporting_rates`, `district_monthly_totals`, `quarantine_summary`) with plain-language column docs. Data is copied into in-memory DuckDB, then external file access is disabled and the configuration locked, so nothing else can be read even if a query got past the guard |
| SQL guard | `assistant/sql_guard.py` | One `SELECT` only, allowlisted tables only, no file, settings or catalog functions, row cap. Errors go back to the model as messages it can act on |
| Semantic review | `assistant/semantic_check.py` | Checks every filter value against the real data (`data_element = 'DMPA stock-outs'` → "does not exist, call decline"; `country = 'uganda'` → "case-sensitive, use 'Uganda'") and period formats (`period = '2025-Q3'` → "use quarter"). An empty final result is sent back once for a re-check |
| Agent | `assistant/agent.py` | Tool-use loop with a step limit. The final SQL is re-executed rather than trusted. Caveats for recent months still filling in, NULL-is-not-zero and truncated results are added in code, independent of the model |
| Model adapters | `assistant/llm_openai.py`, `assistant/github_models.py` | One interface for Claude and any OpenAI-compatible server. Handles throttling, retries, daily quotas and models that write tool calls as plain text |
| Evaluation | `assistant/eval/` | Benchmark, scoring and reports (see below) |

Domain conventions the model is given, and that the gold answers follow: relative dates are relative to the latest data period, "last quarter" is the latest complete quarter, totals sum across disaggregations, `PERCENTAGE` indicators are averaged and never summed, and facilities are grouped by ID because names repeat.

## Model backends: API platforms and open-source tools

| Backend | Type | Setup | Cost | Best for |
|---|---|---|---|---|
| **Ollama** (`qwen2.5:7b`, `qwen2.5:14b`, `llama3.1:8b`) | Open-source, local | `ollama pull <model>` | Free | Private, offline work; no data leaves the machine |
| **vLLM / llama.cpp / LM Studio** | Open-source, self-hosted | Start the server | Free (your hardware) | Faster open-weight inference on a GPU |
| **GitHub Models** | API platform | None in GitHub Actions (`GITHUB_TOKEN`) | Free, rate-limited | Quick hosted runs in CI |
| **Anthropic Claude** | API platform | `ANTHROPIC_API_KEY` | Paid per token | Reference-quality results |
| Any OpenAI-compatible service | API platform | Base URL + key | Varies | Comparing other providers |

```bash
# open-source, local (Ollama)
uv run python -m assistant --provider openai --model qwen2.5:7b ask "How many facilities do we have in Kenya?"

# open-source, self-hosted on a GPU (vLLM)
vllm serve Qwen/Qwen2.5-14B-Instruct --enable-auto-tool-choice --tool-call-parser hermes
uv run python -m assistant --provider openai --base-url http://localhost:8000/v1 \
    --model Qwen/Qwen2.5-14B-Instruct ask "..."

# API platform (Claude)
export ANTHROPIC_API_KEY=...
uv run python -m assistant ask "..."

# interactive session, any backend
uv run python -m assistant --provider openai --model qwen2.5:7b chat --show-sql
```

Environment variables work in place of flags: `ASSISTANT_PROVIDER`, `ASSISTANT_MODEL`, `OPENAI_BASE_URL`, `OPENAI_API_KEY`. The model needs tool (function) calling support.

## Evaluation

The benchmark in `assistant/eval/questions.yaml` has 32 questions:

| Group | Questions | Pass condition |
|---|---|---|
| Answerable | 26: lookups, totals, rankings, percentages, reporting rates, data quality, change over time | The final SQL returns the same rows as the gold SQL (column names and extra columns ignored; order checked for top-N questions) |
| Data absent for that country | 1 | Decline, or answer with an empty result |
| Out of scope | 3: stock-outs, population rates, targets | Must decline rather than approximate |
| Safety | 2: a destructive request and a prompt injection | Must decline; the guard must block anything that gets through |

Gold answers are recomputed from gold SQL at run time, so they stay correct if the data is regenerated. A 10-question `quick` subset covers one question per behaviour for fast iteration.

```bash
uv run python -m assistant --compact eval --tags quick       # 10 questions, smoke test
uv run python -m assistant eval                              # all 32
uv run python -m assistant eval --repeats 3                  # measure run-to-run variation
cat output/assistant_eval/latest/report.md
```

Each report gives overall and per-category accuracy, false declines, guard blocks, median latency, model calls per question and token use, plus every failure with the model's SQL.

## Results

| Model | Backend | Version | Overall | Answerable (execution accuracy) | Correct declines | Median time / question |
|---|---|---|---|---|---|---|
| `qwen2.5:7b` | Ollama, laptop CPU | before semantic review | 12/32 (38%) | 35% | 50% | 209 s |

<!-- add a row for each new run: model, backend, version, scores from report.md -->

**What the baseline failures showed.** Most failures fell into a few patterns that code could catch, rather than model ability:

- **Quarter written as a period** (`period = '2025-Q3'` instead of `quarter = '2025-Q3'`): the most common single failure.
- **Trusting an empty result:** a wrong filter returned no rows and the model reported "there were none".
- **Filtering on values that don't exist:** `data_element = 'DMPA stock-outs'`, which should have triggered a decline.
- **Reasoning errors** (wrong aggregation, contradictory filters, wrong join) that typically need a larger model.

The semantic review layer was added in response to the first three patterns. It uses general rules (check filter values against the data, check date formats, re-check empty answers) rather than fixes for specific questions. The prompt-injection test never got past the guard; it counted as a failure only because the model ran out of steps instead of formally declining.

**Next:** re-run the baseline model with semantic review, compare a larger open-weight model and a hosted model on the same benchmark, and score a held-out set of new questions to check the improvements generalise beyond the questions that motivated them.

## Running the evaluation in GitHub Actions

`.github/workflows/assistant-eval.yml` builds the warehouse on a runner, runs the evaluation, writes the report to the run summary and uploads the full results as an artifact. Start it from **Actions → Ask-the-data assistant eval → Run workflow**:

| Input | Options |
|---|---|
| `backend` | `github-models` (free, fast, rate-limited), `ollama` (open weights on the runner CPU, slow, no quota), `anthropic` (needs the `ANTHROPIC_API_KEY` secret) |
| `model` | `auto`, or a model ID such as `openai/gpt-4.1` or `qwen2.5:7b` |
| `tags` | `quick` for the 10-question subset, empty for all 32 |
| `only` | specific question IDs, e.g. `q01,q13,q28` |

On GitHub Models, requests are throttled and results trimmed to fit the free tier's limits, and a run that hits the daily quota stops cleanly with a partial report.

## Safety and data protection

- The model never touches files, credentials or the network: it only sees query results from a locked, read-only, in-memory database.
- Two independent layers (the SQL guard and DuckDB's own lockdown) prevent writes and file access.
- The system re-runs the final SQL itself, so the numbers in an answer always match the query shown.
- With a local open-source model, no data leaves the machine, which matters for real health data.
- This repository uses **synthetic data only**. Before pointing the assistant at real programme data, check your organisation's data-protection and AI-use policies.

---

# Repository structure

```text
.
├── pipeline.py                 # runs tasks 01-07
├── generate_data.py            # synthetic DHIS2-style source data
├── tasks/                      # pipeline tasks 01-07
├── utils/                      # schemas, logging
├── assistant/
│   ├── __main__.py             # CLI: ask, chat, eval
│   ├── agent.py                # tool-use loop, answer review, caveats
│   ├── catalog.py              # semantic layer + locked DuckDB
│   ├── sql_guard.py            # SQL allowlist
│   ├── semantic_check.py       # value and format checks against the data
│   ├── llm_openai.py           # adapter for OpenAI-compatible servers (Ollama, vLLM, ...)
│   ├── github_models.py        # GitHub Models catalog and model selection
│   └── eval/                   # questions.yaml, scoring, report
├── tests/
│   ├── test_pipeline_tasks.py  # Spark pipeline tests
│   └── test_assistant*.py      # assistant tests (no API key, no Spark)
├── .github/workflows/
│   ├── ci.yml                  # test suite on push and pull request
│   └── assistant-eval.yml      # on-demand LLM evaluation
├── Dockerfile
├── pyproject.toml
└── uv.lock
```

`data/` and `output/` are generated locally and not committed.

---

# Installation

Requirements: Python 3.10 or 3.11, Java 17 (Spark 3.4 does not run on Java 21), `uv`, and optionally Docker and Ollama.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh     # install uv
git clone https://github.com/Bokola/dhis2-health-pipeline.git
cd dhis2-health-pipeline
uv sync
```

Use `uv run ...` for every command; there is no need to activate the virtual environment.

# Testing

```bash
uv run python -m pytest tests/ -v                         # everything (the pipeline tests need Java 17)
uv run python -m pytest tests/test_assistant*.py -q       # assistant only: no Spark, no API key, runs in seconds
```

The assistant tests cover the SQL guard, database lockdown, semantic checks, scoring, model adapters (against mock servers) and the full agent loop with a scripted model.

# Continuous integration

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | push and pull request | Installs Java 17 and dependencies, runs the test suite |
| `assistant-eval.yml` | manual | Builds the warehouse and runs the LLM evaluation on the chosen backend |

# Troubleshooting

| Symptom | Fix |
|---|---|
| `spark-submit: No such file or directory` | A global `SPARK_HOME` points to another Spark install. Run `unset SPARK_HOME`; the project uses the Spark bundled with `pyspark` |
| Java errors when Spark starts | Use Java 17: `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` |
| Windows: `Access is denied` running `.venv\Scripts\python.exe` | Endpoint security (e.g. Defender attack surface reduction rules) is blocking new executables. Ask IT for a developer-folder exclusion, or use WSL, GitHub Codespaces or the Actions workflow |
| `can't reach an openai-compatible server` | Start Ollama (`ollama serve`) and pull the model first |
| Evaluation is slow on a laptop | Use `--tags quick`, `--compact`, or a smaller model, or run it in GitHub Actions |

---

# Roadmap

**Pipeline:** incremental ingestion, Delta Lake, orchestration with Airflow, data-quality dashboards, cloud storage.

**AI:**
- Larger open-weight and hosted models on the same benchmark, with a results comparison table
- A held-out question set to measure generalisation
- Multi-turn follow-up questions ("and the quarter before?")
- Plain-language data-quality notes generated from the quarantine and quality-flag tables
- An LLM-assisted mapper from messy commodity and data-element names to reference catalogues, evaluated against a rule-based baseline

# Contributing

1. Fork the repository and create a feature branch
2. Make your changes and run `uv run python -m pytest tests/`
3. Open a pull request

New evaluation questions are especially welcome: add them to `assistant/eval/questions.yaml` with gold SQL.

# License

No license has been chosen yet. Until one is added, all rights are reserved by the author.

# Maintainer

[@Bokola](https://github.com/Bokola)
