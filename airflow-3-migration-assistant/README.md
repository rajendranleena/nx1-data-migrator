# Airflow 3 Migration Assistant

A static-analysis tool that scans Apache Airflow 2 DAG files and either reports or automatically applies the changes required for Airflow 3 compatibility. Outputs a detailed HTML report that summarises every finding by file, category, and severity.

-----

## Features

- **Dry-run mode** — scan DAGs and produce a full report without touching any files (default)
- **Apply mode** — automatically rewrites fixable issues in-place, with optional `.airflow2.bak` backups
- **HTML report** — styled summary with per-file breakdowns, category tags, and a detailed findings table
- **Wide rule coverage** across all major Airflow 3 breaking changes (see [What It Detects](#what-it-detects))

-----

## Requirements

- Python 3.8+
- No third-party packages required — uses only the standard library

-----

## Installation

```bash
# Clone or copy the single script into your project
wget https://your-repo/airflow3_migration_assistant.py

# Or copy it alongside your DAGs directory
cp airflow3_migration_assistant.py /path/to/project/
```

-----

## Usage

```bash
# Dry run — scan and report, no files modified (default)
python airflow3_migration_assistant.py /path/to/dags --dry-run

# Apply all auto-fixable changes
python airflow3_migration_assistant.py /path/to/dags --apply

# Custom report output path
python airflow3_migration_assistant.py /path/to/dags --dry-run --report-output ./migration_report.html

# Apply without creating backup files (not recommended)
python airflow3_migration_assistant.py /path/to/dags --apply --no-backup
```

### Arguments

|Argument               |Description                                                                              |
|-----------------------|-----------------------------------------------------------------------------------------|
|`dag_directory`        |Path to the directory containing Airflow DAG files (required)                            |
|`--dry-run`            |Scan and report only; do not modify files (default behaviour)                            |
|`--apply`              |Apply all auto-fixable changes to DAG files                                              |
|`--no-backup`          |Skip creating `.airflow2.bak` backup files when applying fixes                           |
|`--report-output`, `-o`|Path to write the HTML report (default: `<dag_directory>/airflow3_migration_report.html`)|

-----

## What It Detects

### SDK Migration (`airflow.sdk`)

Detects imports that have moved from scattered sub-modules into the unified `airflow.sdk` namespace, including `DAG`, `BaseOperator`, `Variable`, `TaskGroup`, `Context`, `Connection`, `BaseHook`, `BaseSensorOperator`, and more.

### Standard Provider

Detects operators and sensors that were bundled with Airflow 2 core but now ship in `apache-airflow-providers-standard`:

- `BashOperator`, `PythonOperator`, `EmptyOperator`, `TriggerDagRunOperator`
- `ExternalTaskSensor`, `FileSensor`, `TimeSensor`, `DateTimeSensor`, and others
- `DummyOperator` → `EmptyOperator` (breaking rename)
- `SubDagOperator` → removed; must refactor to `TaskGroup`

### Assets Rename

`Dataset` and related classes were renamed throughout Airflow 3:

|Airflow 2       |Airflow 3     |
|----------------|--------------|
|`Dataset`       |`Asset`       |
|`DatasetAlias`  |`AssetAlias`  |
|`DatasetAll`    |`AssetAll`    |
|`DatasetAny`    |`AssetAny`    |
|`DatasetEvent`  |`AssetEvent`  |
|`DatasetManager`|`AssetManager`|

### DAG Parameters

|Removed            |Replacement             |
|-------------------|------------------------|
|`schedule_interval`|`schedule`              |
|`timetable`        |`schedule`              |
|`task_concurrency` |`max_active_tis_per_dag`|
|`fail_stop`        |`fail_fast`             |

### Context Variables

Detects access to removed or renamed template context keys: `execution_date` → `logical_date`; `tomorrow_ds`, `yesterday_ds`, `prev_ds`, `next_ds`, and related `_nodash` variants flagged as requiring manual remediation.

### SQL Operators

`TrinoOperator`, `PostgresOperator`, `MySqlOperator`, `MsSqlOperator`, `SnowflakeOperator`, and `HiveOperator` are all removed. Detects their imports and instantiations and replaces them with `SQLExecuteQueryOperator` from `apache-airflow-providers-common-sql`. Connection parameter keywords (e.g. `trino_conn_id`, `postgres_conn_id`) are also rewritten to `conn_id`.

### Removed Utilities & Features

- `days_ago()` from `airflow.utils.dates` → `pendulum.today('UTC').add(days=-N)`
- Entire `airflow.utils.dates` module removed
- SLA (`sla=`, `sla_miss_callback=`) removed; flagged as manual-fix required
- `use_dill=` parameter removed from `PythonOperator`
- `SubDagOperator` removed

### Provider-Specific Changes

- **Kubernetes**: `KubernetesPodOperator` path changed from `.kubernetes_pod` to `.pod`
- **Google**: `delegate_to=` → `impersonation_chain=`; `TaskMixin` → `DependencyMixin`
- **AWS**: `.get_hook()` → `.hook` property
- **Auth**: `AirflowSecurityManager` and `FabAuthManager` moved to FAB provider package

### Direct Database Access (Breaking)

Airflow 3 blocks tasks from accessing the metadata database directly. Detects:

- `create_session` / `provide_session` imports
- `Session()` instantiation
- `DagRun.find()` calls
- `.get_previous_dagrun()` calls

All flagged as **MANUAL FIX REQUIRED** — use the [Apache Airflow REST API client](https://github.com/apache/airflow-client-python) instead.

### Trigger Rules

|Removed                             |Replacement                              |
|------------------------------------|-----------------------------------------|
|`TriggerRule.NONE_FAILED_OR_SKIPPED`|`TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`|
|`TriggerRule.DUMMY`                 |`TriggerRule.ALWAYS`                     |

-----

## Output

### Console Summary

```
======================================================================
  AIRFLOW 3 MIGRATION ASSISTANT - SCAN RESULTS
======================================================================
  Mode:              DRY RUN (no changes made)
  Files scanned:     12
  Files with issues: 7
  Total findings:    43
  Breaking changes:  31
  Deprecations:      10
  Auto-fixable:      36
----------------------------------------------------------------------

  Issues by category:
    SDK Migration                  14
    Standard Provider              11
    Assets Rename                   8
    ...
======================================================================
```

### HTML Report

Generated at `<dag_directory>/airflow3_migration_report.html` (or the path specified with `-o`). Contains:

- Run metadata (directory, timestamp, mode)
- Summary cards for files scanned, total findings, breaking/deprecated/warning counts, and auto-fix status
- Issues-by-category tag cloud
- Per-file breakdown table
- Full findings table with old code, new code, description, and fix status per line

-----

## Severity Levels

|Level       |Meaning                                                                     |
|------------|----------------------------------------------------------------------------|
|`BREAKING`  |Will cause runtime errors in Airflow 3; must be resolved                    |
|`DEPRECATED`|Works via compatibility shim in early Airflow 3 releases but will be removed|
|`WARNING`   |Informational; review recommended                                           |

-----

## Backup & Safety

When running with `--apply`, each modified file is backed up as `<original>.airflow2.bak` before any changes are written. Pass `--no-backup` to skip this step (not recommended for first-time runs).

Files and directories automatically excluded from scanning:

- `__pycache__`
- `.git`
- `airflow3_migration_assistant.py` itself
- Any file ending in `.airflow2.bak`

-----

## Limitations

- The tool operates on individual lines using regex. Multi-line expressions (e.g. a DAG constructor split across several lines) may not be fully resolved by auto-fix; review the HTML report for any `MANUAL` status findings.
- SQL operator migration (`PostgresOperator` → `SQLExecuteQueryOperator`) rewrites the class name and connection parameter but does not restructure keyword arguments beyond `conn_id` renaming. Verify operator kwargs manually after applying.
- Direct database access patterns are detected but **not** auto-fixed; they require architectural changes to use the REST API.

-----

## License

MIT
