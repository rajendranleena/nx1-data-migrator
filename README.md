# nx1-data-migrator

Data platform migration tools (MapR/HDFS to S3/Iceberg) and access control automation.

## Repository Structure

| Directory | Description |
|-----------|-------------|
| `data-iceberg-migrator/` | Airflow DAGs for migrating Hive tables from MapR-FS/HDFS to S3 and converting to Iceberg format |
| `ranger-policies-generator/` | Airflow DAG for automating Apache Ranger policies and Keycloak role mappings from Excel config |
| `code-scanner/` | Standalone CLI tool for static analysis of Spark, HDFS, JDK, and Python migration patterns |

## Development Setup

**Python 3.12** is required.

```bash
python -m venv .venv
source .venv/bin/activate
pip install ".[dev]"
```

## Running Tests

Each project has its own test suite. Run from the project directory:

```bash
cd data-iceberg-migrator
pytest tests/           # fast, no coverage
pytest tests/ --cov     # with coverage

cd ../ranger-policies-generator
pytest tests/
pytest tests/ --cov
```

Coverage settings (source module, 80% threshold) are in each project's `.coveragerc`. Test settings (`-v`, `--timeout=60`) are in each project's `pytest.ini`.

## CI / GitHub Actions

A unified workflow (`.github/workflows/ci.yml`) runs on every push to `main` and on all pull requests targeting `main`.

### Pipeline

```
lint (ruff check + format) ─┬─► test-data-iceberg-migrator ─┬─► coverage-report (PR comment)
                            └─► test-ranger-policies-generator ─┘
```

### Jobs

| Job | What it does |
|-----|-------------|
| **lint** | Runs `ruff check` and `ruff format --check` across the entire repo |
| **test-data-iceberg-migrator** | Installs dev deps, runs pytest with coverage, uploads coverage artifact |
| **test-ranger-policies-generator** | Same as above for the ranger project |
| **coverage-report** | Downloads coverage artifacts, posts a summary comment on the PR |

### Coverage

- Each test job enforces an **80% minimum** coverage threshold (configured in `.coveragerc`)
- On PRs, the `coverage-report` job posts a coverage summary as a PR comment
- Coverage XML artifacts are uploaded for each project
