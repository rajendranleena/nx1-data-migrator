# DAG 5: Iceberg Rewrite Table Path Migration

## Prerequisite Conditions

Before using this DAG, ensure the following conditions are met:

- Both **data files** and **metadata files** for all tables to be migrated have already been copied to the destination S3 bucket.
- The destination Spark/Iceberg environment is running Apache Iceberg **1.4.0 or newer** and supports the `rewrite_table_path` stored procedure.
- You have the required S3 credentials and access permissions for both reading and writing to the relevant S3 paths.
- The configuration Excel file is available in S3 and contains the necessary columns: `database`, `table` (optional), `source_s3_prefix`, and `dest_s3_prefix`.
- Any required Airflow Variables or environment variables (such as tracking/report locations and S3 credentials) are set and accessible by the DAG.

**Note:**

This DAG supports both full snapshot and incremental migrations. For incremental loads, you must manually re-copy any new or changed data files and the corresponding updated metadata files to the destination S3 before each run. The DAG will then rewrite all metadata paths for the current state at the destination. Automatic detection or syncing of incremental changes is not handled by this DAG; it operates on whatever data and metadata are present at the destination when triggered.


Iceberg-to-Iceberg migration using the `rewrite_table_path` stored procedure. Use this DAG when data **and** metadata have already been copied to the destination S3 bucket and snapshot history / partition transform fidelity must be preserved.

---



## Strategy: `iceberg_rewrite_table_path`

The procedure reads the existing Iceberg metadata at the destination — which still references the old source S3 path prefixes — and rewrites every path reference via `CALL spark_catalog.system.rewrite_table_path`. The procedure writes the rewritten metadata directly to `dest_path/metadata` (derived by applying the same prefix rewrite to the table's `location` field). The table is then permanently registered in HMS via `register_table` using that new metadata.

**Preserved by this approach:**
- Full snapshot history and time-travel
- All partition transforms (`year`, `month`, `bucket`, `truncate`, etc.)
- Exact schema types — no DDL type translation
- All table properties

---

## Requirements

- Both **data files** and **metadata files** must be pre-copied to the destination S3 path before running this DAG.
- The destination Spark/Iceberg environment must support `rewrite_table_path` — Apache Iceberg **1.4+** required.
- `source_s3_prefix` and `dest_s3_prefix` must be provided in the Excel config (see below).

---

### Required Variables

| Airflow Variable key | Env var fallback | Description | Example |
|---|---|---|---|
| `migration_tracking_database` | `MIGRATION_TRACKING_DATABASE` | Database for Iceberg tracking tables | `migration_tracking` |
| `migration_tracking_location` | `MIGRATION_TRACKING_LOCATION` | S3 location for tracking tables | `s3a://data-lake/migration_tracking` |
| `migration_report_location` | `MIGRATION_REPORT_LOCATION` | S3 location where HTML reports are written | `s3a://data-lake/migration_reports` |

### Optional Variables

| Airflow Variable key | Env var fallback | Default | Description |
|---|---|---|---|
| `migration_email_recipients` | `MIGRATION_EMAIL_RECIPIENTS` | _(empty)_ | Comma-separated list of report recipients; email is skipped if empty |
| `migration_smtp_conn_id` | `MIGRATION_SMTP_CONN_ID` | `smtp_default` | Airflow SMTP connection ID |

### S3 Credentials

DAG 5 only accesses the **destination** S3 bucket at runtime. The `source_s3_prefix` is used purely as a text prefix to construct the source metadata file path — Spark never opens a connection to source S3. Source S3 credentials are not required.

**Destination credentials** — set the dest-specific variables; the global variables are used as fallback if they are not set:

| Airflow Variable key | Env var fallback | Description |
|---|---|---|
| `s3_dest_endpoint` | `S3_DEST_ENDPOINT` | Destination S3 endpoint URL |
| `s3_dest_access_key` | `S3_DEST_ACCESS_KEY` | Destination S3 access key |
| `s3_dest_secret_key` | `S3_DEST_SECRET_KEY` | Destination S3 secret key |

**Global S3 fallback** (used when dest-specific variables are not set):

| Airflow Variable key | Env var fallback | Description |
|---|---|---|
| `s3_endpoint` | `S3_ENDPOINT` | Global S3 endpoint URL |
| `s3_access_key` | `S3_ACCESS_KEY` | Global S3 access key |
| `s3_secret_key` | `S3_SECRET_KEY` | Global S3 secret key |

Credentials are applied at the per-bucket level internally via Hadoop's `fs.s3a.bucket.<name>.*` properties — no additional configuration is needed beyond setting the variables above.

### DAG Parameter

| Parameter | Required | Description | Example |
|---|---|---|---|
| `excel_file_path` | Yes | S3 path to Excel config file | `s3a://config-bucket/iceberg_rewrite_migration.xlsx` |

---

## Excel Configuration Format

| Column | Required | Description | Example |
|---|---|---|---|
| `database` | **Yes** | HMS database name (same for source and destination) | `analytics` |
| `table` | No | Table name(s) — single, comma-separated, or wildcard; defaults to `*` | `orders` or `trans*` or `*` |
| `source_s3_prefix` | **Yes** | Original source S3 prefix (the prefix embedded in the pre-copied metadata files) | `s3a://source-bucket/warehouse` |
| `dest_s3_prefix` | **Yes** | Destination S3 prefix (what the metadata paths should be rewritten to) | `s3a://dest-bucket/warehouse` |

Rows are grouped by `(database, source_s3_prefix, dest_s3_prefix)`. Multiple rows for the same group accumulate their table tokens.

**Example:**

```
| database  | table      | source_s3_prefix            | dest_s3_prefix             |
|-----------|------------|-----------------------------|----------------------------|
| analytics | orders     | s3a://source-bucket/warehouse | s3a://dest-bucket/warehouse |
| analytics | customers  | s3a://source-bucket/warehouse | s3a://dest-bucket/warehouse |
| reporting | *          | s3a://source-bucket/warehouse | s3a://dest-bucket/warehouse |
```

---

## Task Flow

```
init_rewrite_tracking_tables
    ↓
create_rewrite_migration_run
    ↓
parse_rewrite_excel
    ↓
┌───────────────────────────────────────────────────────────────────┐
│  Dynamic Task Mapping (one set of tasks per database config)      │
│                                                                   │
│  discover_rewrite_tables (PySpark)                                │
│    ↓                                                              │
│  record_rewrite_discovered_tables  [trigger: all_done]            │
│    ↓                                                              │
│  validate_rewrite_data_presence  [trigger: all_done]              │
│    ↓                                                              │
│  update_rewrite_data_presence_status  [trigger: all_done]         │
│    ↓                                                              │
│  create_rewrite_dest_tables  [trigger: all_done]                  │
│    ↓                                                              │
│  update_rewrite_table_create_status  [trigger: all_done]          │
│    ↓                                                              │
│  validate_rewrite_destination_tables  [trigger: all_done, max=3]  │
│    ↓                                                              │
│  update_rewrite_validation_status  [trigger: all_done]            │
└───────────────────────────────────────────────────────────────────┘
    ↓
generate_rewrite_html_report  [trigger: all_done]
    ↓
send_rewrite_report_email  [trigger: all_done]
    ↓
finalize_rewrite_run  [trigger: all_done]
```

---

## Task Summaries

### Step 0 — `init_rewrite_tracking_tables`

**Type:** PySpark

- Creates the tracking database if it does not exist.
- Creates two Iceberg tracking tables if they do not exist:
  - `rewrite_migration_runs` — run-level metadata (run ID, status, counts, timestamps)
  - `rewrite_migration_table_status` — table-level tracking partitioned by `source_database`

---

### Step 1 — `create_rewrite_migration_run`

**Type:** PySpark

- Generates a unique run ID in the format `rewrite_run_{YYYYMMDD_HHMMSS}_{uuid8}`.
- Inserts an initial `RUNNING` record into `rewrite_migration_runs`.
- Returns the `run_id` used by all downstream tasks.

---

### Step 2 — `parse_rewrite_excel`

**Type:** PySpark

- Reads the Excel file from S3 using `pyspark.pandas.read_excel`.
- Normalizes column names (lowercase, strip whitespace).
- Groups rows by `(database, source_s3_prefix, dest_s3_prefix)`.
- Supports single table names, comma-separated lists, and wildcard patterns in the `table` column.
- Returns a list of database-config dicts for dynamic task mapping.
- Raises if no valid rows are found.

---

### Step 3 — `discover_rewrite_tables`

**Type:** PySpark (mapped per database config) · **@track_duration**

- Reads schema, partition spec, row count, and file stats from `metadata.json` at the **destination** S3 path (not from HMS — HMS may not have the table registered yet and would not preserve partition transform details).
- Lists tables under `{dest_s3_prefix}/{database}/` and filters by the `table_tokens` pattern.
- On per-table failure, records an error entry and raises after processing all tables.

---

### Step 4 — `record_rewrite_discovered_tables`

**Type:** PySpark (mapped per database config) · trigger: `all_done`

- Inserts or updates records in `rewrite_migration_table_status` with schema, partition spec, file count, and size from discovery.
- Sets `discovery_status = COMPLETED` and `overall_status = DISCOVERED`.

---

### Step 5 — `validate_rewrite_data_presence`

**Type:** PySpark (mapped per database config) · trigger: `all_done`

- Uses the Hadoop FileSystem API to verify each destination table path:
  - Path must exist.
  - A `metadata/` subdirectory must be present (required for `rewrite_table_path`).
  - At least one file must be present.
- Sets status `CONFIRMED`, `MISSING`, or `FAILED` per table.
- Raises only on `FAILED` (API/connectivity errors); `MISSING` tables continue and are tracked.

---

### Step 6 — `update_rewrite_data_presence_status`

**Type:** PySpark (mapped per database config) · trigger: `all_done`

- Updates `data_presence_status`, `data_presence_file_count`, `data_presence_size_bytes`, and `overall_status` in the tracking table.
- Tables with `DATA_MISSING` are skipped by all downstream steps but remain visible in the report.

---

### Step 7 — `create_rewrite_dest_tables`

**Type:** PySpark (mapped per database config) · trigger: `all_done` · **@track_duration**

Skips tables where data presence is not `CONFIRMED`. For each confirmed table, executes a 5-step pipeline:

1. **Drop from HMS** if already registered (no `PURGE` — data files are preserved).
2. **Temporarily register** using the **source** metadata file: constructs the equivalent source path by swapping `dest_s3_prefix` → `source_s3_prefix` in `dest_path`, resolves the metadata file there, and calls `CALL spark_catalog.system.register_table(table, source_metadata_file)`. This is required so `rewrite_table_path` sees a `location` field that starts with `source_s3_prefix` and knows which paths to rewrite.
3. **Rewrite metadata**: `CALL spark_catalog.system.rewrite_table_path(table, source_prefix, target_prefix, staging_location)` — rewrites all `source/` path references to `dest/` and writes the new metadata directly to `dest_path/metadata`.
4. **Drop the temporary registration**.
5. **Permanently register in HMS** via `CALL spark_catalog.system.register_table(table, metadata_file)` using `_resolve_metadata_file(spark, dest_path)` — picks up the newly written metadata at `dest_path/metadata`.

`staging_location` (`{dest_s3_prefix}/{dest_database}/_staging_rewrite/{table_name}`) is passed to `rewrite_table_path` as required by the procedure but is used internally for coordination only — it is not the output directory. It is cleaned up after successful registration, and on failure.

On failure, staging is cleaned up automatically. The task raises after all tables are processed if any failed.

---

### Step 8 — `update_rewrite_table_create_status`

**Type:** PySpark (mapped per database config) · trigger: `all_done`

- Updates `table_create_status`, `table_create_duration_seconds`, `table_already_existed`, and `overall_status` for each table.
- Applies a catch-all update to mark any unprocessed `CONFIRMED` tables as `FAILED`.

---

### Step 9 — `validate_rewrite_destination_tables`

**Type:** PySpark (mapped per database config) · trigger: `all_done` · max 3 concurrent · **@track_duration**

For each table that was successfully created:
- Queries `SELECT COUNT(*)` and `.partitions` on the destination table.
- Compares against source row count and partition count stored in the tracking table.
- Performs schema comparison between source `metadata.json` schema and `DESCRIBE` output.
- Partition count mismatches are treated as warnings.
- Schema and row count mismatches are failures.

---

### Step 10 — `update_rewrite_validation_status`

**Type:** PySpark (mapped per database config) · trigger: `all_done`

- Updates `validation_status`, `row_count_match`, `partition_count_match`, `schema_match`, `schema_differences`, and `overall_status` (`VALIDATED` or `VALIDATION_FAILED`).
- Applies catch-all updates for tables that were not processed by the validation task.

---

### Step 11 — `generate_rewrite_html_report`

**Type:** PySpark · trigger: `all_done`

Generates an HTML report and writes it to `{report_output_location}/{run_id}_rewrite_report.html`.

Report sections:
1. **Migration Summary** — total/validated/failed/missing tables, source rows, destination data size
2. **Data Presence & Metadata Check** — per-table presence status, file count, size
3. **Table Migration Details** — per-table overall status, format, partitioning, and task durations
4. **Validation Results** — row count, partition count, and schema match per table

---

### Step 12 — `send_rewrite_report_email`

**Type:** PySpark · trigger: `all_done`

- Reads the HTML report from S3 and sends it as an email attachment.
- Subject: `Iceberg Rewrite Migration Report — {run_id}`.
- Skips silently if `email_recipients` is not configured.
- Uses the `smtp_conn_id` Airflow connection (default: `smtp_default`).

---

### Step 13 — `finalize_rewrite_run`

**Type:** PySpark · trigger: `all_done`

- Aggregates final counts from `rewrite_migration_table_status`.
- Updates `rewrite_migration_runs` with `completed_at` and final status.

**Final run statuses:**

| Status | Meaning |
|---|---|
| `COMPLETED` | All tables validated, no failures, no missing |
| `COMPLETED_WITH_MISSING` | No failures but some tables had no data at destination |
| `COMPLETED_WITH_FAILURES` | One or more tables failed at any stage |
| `FAILED` | Run-level error (tracking query failed) |

---

## Status Progression

**Per-table `overall_status`:**

```
DISCOVERED
    ↓
DATA_CONFIRMED  (metadata and data files found at destination)
    ↓
TABLE_CREATED  (rewrite_table_path + register_table executed)
    ↓
VALIDATED  (row count, partition count, schema all match)

DATA_MISSING → skipped in all downstream steps, visible in report
(Any stage) → FAILED or VALIDATION_FAILED
```

| Status | Meaning |
|---|---|
| `DISCOVERED` | Metadata read from destination, tracking record inserted |
| `DATA_CONFIRMED` | Data and metadata files present at destination S3 |
| `DATA_MISSING` | No files or missing `metadata/` directory — skipped |
| `TABLE_CREATED` | `rewrite_table_path` + `register_table` completed, validation pending |
| `VALIDATED` | All validations passed — migration success |
| `VALIDATION_FAILED` | Row count, partition count, or schema mismatch |
| `FAILED` | Error at discovery, data presence check, or table creation |

---

## Tracking Tables

| Table | Description |
|---|---|
| `{tracking_database}.rewrite_migration_runs` | One row per DAG run — run ID, status, counts, timestamps, config snapshot |
| `{tracking_database}.rewrite_migration_table_status` | One row per table per run — discovery, data presence, table creation, and validation results; partitioned by `source_database` |

---

## DAG ID & Tags

| Property | Value |
|---|---|
| DAG ID | `iceberg_rewrite_table_path_migration` |
| Tags | `migration`, `iceberg`, `rewrite-table-path`, `approach-2` |
| Max active runs | 5 |
| Schedule | Manual (`None`) |
| Retries | 2, 5-minute delay |
