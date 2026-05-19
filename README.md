# DHIS2 Health Data Pipeline Infrastructure

This engine parses, cleanses, transforms, and indexes complex health service delivery logs from DHIS2 tracking contexts into structured dimensional models.

## Tech Stack Architecture
- **Language Environment**: Python 3.10
- **Package and Dependency Orchestration**: `uv` project workspace structure
- **Distributed Transformation Engine**: PySpark 3.4.1 (Local execution thread configuration)
- **Containerization Platform**: Multi-stage Docker environment with OpenJDK-17 runtime layers
- **CI/CD Orchestration Layer**: GitHub Workflows compiling and verifying execution integrity on push boundaries

## Local Initialization and Execution Flow

### 1 Local execution via uv
Make sure you have `uv` installed inside your terminal environment:
```bash
pip install uv
uv lock
uv sync
```
To populate synthetic data logs and run your transformations sequentially:

```bash
# generate your mock dataset
uv run generate_data.py --out ./data

# execute the pipeline driver sequence
uv run pipeline.py --data-dir ./data --output-dir ./output
```

### Docker container automation
To verify, build, and isolate your processing space within container profiles locally:

```bash
docker build -t dhis2-pipeline:latest .
docker run --rm dhis2-pipeline:latest
# run pipeline
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 uv run pipeline.py --data-dir ./data --output-dir ./output
# run test
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 uv run python -m pytest tests/ -v
```