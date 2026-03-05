# MapR to S3 Migration DAG

An automated **Airflow TaskFlow-based migration pipeline** consisting of two independent DAGs for orchestrating large-scale Hive table migrations from MapR-FS/HDFS to S3 and converting existing tables to Iceberg format.

---

## Overview

This implementation provides two independent but complementary migration DAGs:

1. **`mapr_to_s3_migration`** - Migrates Hive tables from MapR-FS/HDFS to S3
2. **`iceberg_migration`** - Converts existing Hive tables in S3 to Apache Iceberg format

---

## Configuration Variables

The DAGs rely on Airflow Variables for configuration. Set these before running:

### Required Variables

| Variable                      | Description                                     | Example                              |
| ----------------------------- | ----------------------------------------------- | ------------------------------------ |
| `cluster_ssh_conn_id`         | Airflow SSH connection ID for cluster edge node | `cluster_edge_ssh`                   |
| `migration_default_s3_bucket` | Default S3 bucket for migrations                | `s3a://data-lake`                    |
| `migration_tracking_database` | Database name for tracking tables               | `migration_tracking`                 |
| `migration_tracking_location` | S3 location for tracking tables                 | `s3a://data-lake/migration_tracking` |
| `migration_report_location`   | S3 location for HTML reports                    | `s3a://data-lake/migration_reports`  |
| `migration_spark_conn_id`     | Airflow Spark connection ID                     | `spark_default`                      |

### Authentication Variables

| Variable                   | Description                                       | Required For           |
| -------------------------- | ------------------------------------------------- | ---------------------- |
| `auth_method`              | Authentication method: `mapr`, `kinit`, or `none` | MapR/Kerberos          |
| `mapr_user`                | MapR username used to validate existing ticket    | MapR auth              |
| `mapr_ticketfile_location` | MapR ticket file path                             | MapR auth              |
| `kinit_principal`          | Kerberos principal                                | Kerberos auth          |
| `kinit_keytab`             | Path to Kerberos keytab file                      | Kerberos keytab auth   |
| `kinit_password`           | Kerberos password                                 | Kerberos password auth |

### Optional Variables

| Variable                     | Default          | Description                                  |
| ---------------------------- | ---------------- | -------------------------------------------- |
| `cluster_edge_temp_path`     | `/tmp/migration` | Temporary directory on edge node             |
| `s3_endpoint`                | _(empty)_        | Custom S3 endpoint URL                       |
| `s3_access_key`              | _(empty)_        | S3 access key (if not using IAM)             |
| `s3_secret_key`              | _(empty)_        | S3 secret key (if not using IAM)             |
| `migration_distcp_mappers`   | `50`             | Number of DistCp mappers                     |
| `migration_distcp_bandwidth` | `100`            | Bandwidth limit per mapper (MB/s)            |
| `s3_listing_tool`            | `hadoop`         | Tool for S3 listing: `hadoop` or `boto3`     |
| `migration_smtp_conn_id`     | `smtp_default`   | Airflow SMTP connection ID for email reports |
| `migration_email_recipients` | _(empty)_        | Comma-separated email addresses for reports  |

---

## DAG Parameter Details

| DAG   | Parameter         | Required | Description               | Example                                      |
| ----- | ----------------- | -------- | ------------------------- | -------------------------------------------- |
| DAG 1 | `excel_file_path` | Yes      | S3 path to Excel config   | `s3a://config-bucket/migration.xlsx`         |
| DAG 2 | `excel_file_path` | Yes      | S3 path to Iceberg config | `s3a://config-bucket/iceberg_migration.xlsx` |

---

## Key Features of all DAGs

- **Parallel Processing** - Dynamic task mapping for concurrent migrations
- **Comprehensive Tracking** - All operations tracked in Iceberg tables with detailed metrics
- **Incremental Support** - Resume and update existing migrations
- **Error Recovery** - Per-table error handling with detailed tracking
- **Duration Tracking** - Automatic tracking of task execution times via XCom decorator

---

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│ DAG 1: MapR to S3                                           │
│                                                             │
│ MapR-FS/HDFS (Hive Tables)                                  │
│ │                                                           │
│ │ [PySpark: Metadata Discovery]                             │
│ ▼                                                           │
│ Metadata Discovery                                          │
│ │                                                           │
│ │ [SSH: DistCp - 24h timeout]                               │
│ ▼                                                           │
│ S3 (Raw Data Files)                                         │
│ │                                                           │
│ │ [PySpark: Hive DDL]                                       │
│ ▼                                                           │
│ S3 (Queryable via Hive)                                     │
│ │                                                           │
│ │ [Validation: Row counts, partitions, schema]              │
│ ▼                                                           │
│ Validated & Tracked                                         │
└─────────────────────────────────────────────────────────────┘
│
│ (Independent, typically run after)
▼
┌─────────────────────────────────────────────────────────────┐
│ DAG 2: Iceberg Migration                                    │
│                                                             │
│ S3 (Hive Tables)                                            │
│ │                                                           │
│ │ [PySpark: Table Discovery]                                │
│ ▼                                                           │
│ Hive Metadata Discovery                                     │
│ │                                                           │
│ │ [Spark Procedures: migrate/snapshot]                      │
│ ▼                                                           │
│ S3 (Iceberg Format)                                         │
│ │                                                           │
│ │ [Validation: Row counts, partitions, schema]              │
│ ▼                                                           │
│ Validated & Tracked                                         │
└─────────────────────────────────────────────────────────────┘
```

---

### Migration Strategy Decision Tree

```
Do you need to migrate from MapR-FS/HDFS to S3?
│
├─ YES → Run DAG 1 (mapr_to_s3_migration)
│ │
│ │
│ └─ Need Iceberg format?
│    │
│    └─ YES → Run DAG 2 (iceberg_migration)
│       │
│       └─ No Hive, Only Iceberg → Inline migration
│       │
│       └─ Both Hive and Iceberg → Snapshot migration
│
└─ NO → Already in S3, need Iceberg?
   │
   └─ YES → Run DAG 2 (iceberg_migration) only
```

---

## DAG 1: MapR to S3 Migration

### Purpose

Orchestrates the complete migration of Hive tables from MapR-FS/HDFS to S3, including data transfer, metadata recreation, and validation.

---

### Key Features

- **SSH Operations** - All MapR interactions via SSH to edge node
- **Beeline Discovery** - Automated metadata extraction using HiveServer2
- **Hadoop DistCp** - Efficient bulk data transfer with 24-hour timeout
- **Incremental Support** - Automatic detection and `update` flag usage
- **Partition Support** - Automatic partition discovery and repair
- **Format Preservation** - Supports Parquet, ORC, and Avro
- **Comprehensive Validation** - Row counts, partition counts, schema comparison

---

### Duration Tracking

Tasks decorated with `@track_duration` automatically capture execution time:

- **Mechanism**: Decorator wraps task function and measures start/end time
- **Storage**: Adds `_task_duration` field to task result dictionary
- **XCom**: Duration flows through task dependencies via XCom
- **Tracking**: Saved to tracking tables in `*_duration_seconds` columns

**Tracked tasks:**

- `discover_tables_via_spark_ssh` → `discovery_duration_seconds`
- `run_distcp_ssh` → `distcp_duration_seconds`
- `create_hive_tables` → `table_create_duration_seconds`
- `validate_destination_tables` → `validation_duration_seconds`

---

### Excel Configuration Format

**Required Columns:**

| Column          | Required | Description                                                                                                                                                   | Example                 |
| --------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| `database`      | **Yes**  | Source database name                                                                                                                                          | `sales_data`            |
| `table`         | No       | Table pattern: supports \* wildcards, comma-separated table names (e.g. table1,table2), or one table per row for same database (rows are combined internally) | `transactions_*` or `*` |
| `dest database` | No       | Destination database (defaults to source)                                                                                                                     | `sales_data_s3`         |
| `bucket`        | No       | S3 bucket (defaults to variable)                                                                                                                              | `s3a://data-lake`       |

---

### Bucket Strategy for Multi-Tenant Migrations

When tables in the same database require different target S3 buckets, use a two-phase approach:

- **Run 1: Lift-and-Shift (default bucket):** Migrate the bulk of tables using the default bucket configured in `migration_default_s3_bucket`. Include all standard tables in a single Excel sheet without specifying a bucket column.
- **Run 2: Customized (non-default buckets):** Create a separate Excel sheet containing only the tables that require a different target bucket, with the `bucket` column explicitly set. Run the DAG a second time pointing to this sheet.

---

### Task Flow

```
validate_prerequisites (SSH: connectivity, PySpark, Hive, Hadoop FS checks)
↓
init_tracking_tables
↓
create_migration_run
↓
parse_excel
↓
cluster_login_setup (SSH: cluster authentication)
↓
┌───────────────────────────────────────────────┐
│ Dynamic Task Mapping (per database config)    │
│                                               │
│ discover_tables_via_spark_ssh (SSH: PySpark)  │
│ ↓                                             │
│ record_discovered_tables                      │
│ ↓                                             │
│ run_distcp_ssh (SSH: DistCp, 24h timeout)     │
│ ↓                                             │
│ update_distcp_status                          │
│ ↓                                             │
│ create_hive_tables (PySpark: DDL/Repair)      │
│ ↓                                             │
│ update_table_create_status   │
│ ↓                                             │
│ validate_destination_tables (PySpark)         │
│ ↓                                             │
│ update_validation_status                      │
└───────────────────────────────────────────────┘
↓
generate_html_report
↓
send_migration_report_email (PySpark: Email report)
↓
finalize_run
↓
cleanup_edge (SSH: Cleanup temp files)
```

---

### Task Summaries

#### Step 0.1 - `validate_prerequisites`

**Type:** SSH  
**Purpose:** Validate all required components are available before starting migration

- Connects to the cluster edge node via SSH
- Runs four sequential checks:
  1. **SSH Connectivity** - Verifies SSH connection works with a simple echo command
  2. **PySpark Availability** - Checks `pyspark --version` is accessible on the edge node
  3. **Hive Availability** - Checks `hive --version` is accessible on the edge node
  4. **Hadoop FS** - Verifies `hadoop fs -ls /` executes successfully
- Sources `~/.profile` before each check to ensure environment variables are loaded
- If **all four checks pass**, proceeds with migration
- If **any check fails**, raises an exception with a detailed summary of which checks failed and why, halting the DAG before any tracking tables or run records are created

---

#### Step 0.2 - `init_tracking_tables`

**Type:** PySpark  
**Purpose:** Initialize the migration tracking infrastructure

- Creates the `migration_tracking` database if it doesn't exist
- Creates two Iceberg tables for tracking:
  - `migration_runs` - Run-level metadata (run ID, status, counts, timestamps)
  - `migration_table_status` - Table-level tracking (discovery, DistCp, table creation)
- Ensures tracking tables persist across all migration runs

---

#### Step 1 - `create_migration_run`

**Type:** PySpark  
**Purpose:** Generate unique run identifier and initialize run record

- Creates a unique run ID with timestamp and UUID
- Inserts initial record into `migration_runs` table with status `RUNNING`
- Stores DAG configuration snapshot for audit trail
- Returns run ID for use in downstream tasks

---

#### Step 2 - `parse_excel`

**Type:** PySpark  
**Purpose:** Read and parse Excel configuration file from S3

- Reads Excel file from S3 using `pyspark.pandas.read_excel`
- Normalizes column names (lowercase, strip whitespace, replace spaces with underscores)
- Validates and defaults configuration values:
  - `dest_database` defaults to source database name
  - `bucket` defaults to `migration_default_s3_bucket` variable
  - `table` pattern defaults to `*` (all tables); supports comma-separated table names and multi-row input for the same database (rows are combined into a single database record internally)
- Expands to list of database configurations for dynamic task mapping
- Filters out rows with empty database names

---

#### Step 3 - `cluster_login_setup`

**Type:** SSH  
**Purpose:** Authenticate to the source cluster (MapR or Kerberos) and prepare edge node environment

- Connects to the cluster edge node via SSH
- Authenticates using one of the following methods, based on configuration:
  1. **Kerberos authentication** - Uses `kinit_principal` and `kinit_keytab` or `kinit_password`
  2. **Existing MapR or Kerberos ticket** - Validates and uses existing valid ticket
- Verifies ticket validity with `maprlogin print` or `klist`
- Creates temporary working directory on edge node (`/tmp/migration/{run_id}`)
- Ensures all subsequent SSH operations can access the source filesystem

---

#### Step 4 - `discover_tables_via_spark_ssh`

**Type:** SSH
**Purpose:** Discover table metadata from Hive using PySpark

- Executes on edge node via SSH on each database in Excel config
- Discovers tables matching the pattern (supports `*` wildcards)
- For each table, extracts:
  - **Schema** - Column names and data types
  - **Location** - Source filesystem path (MapR-FS or HDFS)
  - **Format** - Parquet, ORC, or Avro (detected from InputFormat)
  - **Partitions** - Partition spec and count (via `DESCRIBE FORMATTED`)
  - **Partition columns** - Extracted from table metadata
- Generates JSON output with all discovered metadata
- Determines S3 destination path: `{bucket}/{dest_database}/{table_name}`

---

#### Step 5 - `record_discovered_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Persist discovered table metadata in Iceberg tracking table

- Inserts or updates records in `migration_table_status` for each discovered table
- Uses `MERGE` statement to handle both new discoveries and re-runs
- Stores comprehensive metadata: schema JSON, partition list, file format, location
- Sets initial status to `DISCOVERED`
- Enables downstream tasks to access table metadata without re-querying Hive

---

#### Step 6 - `run_distcp_ssh`

**Type:** SSH (mapped per database)  
**Purpose:** Copy data from MapR-FS/HDFS to S3 using Hadoop DistCp

- Executes DistCp via SSH for each table discovered in previous step
- **Incremental detection:**
  - Checks if S3 destination already exists using `hadoop fs -test -d`
  - If exists, runs `hadoop distcp -update` (incremental sync)
  - If new, runs full copy
- **DistCp configuration:**
  - Configurable mapper count (default: 50)
  - Bandwidth limit per mapper (default: 100 MB/s)
  - Dynamic strategy for load balancing
  - S3 credentials passed via `-D` properties
- Captures success/failure status per table
- **File metrics tracking:**
  - Calculates S3 metrics BEFORE DistCp: file count and total size
  - Calculates S3 metrics AFTER DistCp: file count and total size
  - Computes transferred bytes and files (delta between before/after)
  - Compares with source MapR/HDFS metrics:
    - `file_size_match`: True if within 1% tolerance
    - `file_count_match`: True if exact match
  - These metrics help detect incomplete copies even when DistCp reports success
- Logs written to `{temp_dir}/distcp_{run_id}_{src_db}.log`
- **Timeout:** 24 hours per table (configurable via `SSH_COMMAND_TIMEOUT`)

---

#### Step 7 - `update_distcp_status`

**Type:** PySpark (mapped per database)  
**Purpose:** Update tracking table with DistCp results

- Updates `migration_table_status` for each table with:
  - `distcp_status` - COMPLETED or FAILED
  - `distcp_completed_at` - Timestamp
  - `distcp_is_incremental` - Boolean flag
  - `overall_status` - Updated to COPIED or FAILED
  - `error_message` - Error details if failed (truncated to 2000 chars)
- Enables monitoring of data copy progress
- Allows restart of failed tables in subsequent runs

---

#### Step 8 - `create_hive_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Create or repair Hive external tables pointing to S3 data

- Creates destination database if it doesn't exist
- For each table:
  - **If table doesn't exist:** Creates new external Hive table
    - Infers schema from discovered metadata or S3 files
    - Applies partition columns if table is partitioned
    - Sets location to S3 path
    - Uses correct file format (Parquet/ORC/Avro)
  - **If table exists (incremental run):** Runs `MSCK REPAIR TABLE`
    - Discovers new partitions added since last run
    - Updates Hive metastore without recreating table
- Handles both partitioned and non-partitioned tables
- Generates proper DDL with escaped column names and types

---

#### Step 9 - `update_table_create_status`

**Type:** PySpark (mapped per database)  
**Purpose:** Update tracking table with table creation results

- Updates `migration_table_status` for each table with:
  - `table_create_status` - COMPLETED, FAILED, or SKIPPED
  - `table_create_completed_at` - Timestamp
  - `table_already_existed` - Boolean flag
  - `overall_status` - Updated to TABLE_CREATED or FAILED
  - `error_message` - Error details if failed
- Tracks whether table was newly created or repaired
- Enables visibility into table creation/repair operations

---

#### Step 10 - `validate_destination_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Validate destination Hive tables: row counts, partition counts, schema comparison

- For each table:
  - Check if source validation succeeded
  - Get destination row count
  - Get destination partition count
  - Perform schema comparison
  - Perform validation checks

---

#### Step 11 - `update_validation_status`

**Type:** PySpark (mapped per database)  
**Purpose:** Update Iceberg tracking with validation results

- For each table in validation results:
  - Skip if validation not completed
  - Escape and truncates error/schema differences
- Update tracking table
- Determine final overall_status

**Final status meanings:**

- DISCOVERED: Metadata extracted, not yet copied
- COPIED: Data copied to S3, table not yet created
- TABLE_CREATED: Hive table created/repaired, not yet validated
- VALIDATED: All validations passed - MIGRATION SUCCESS
- VALIDATION_FAILED: One or more validations failed
- FAILED: DistCp or table creation failed

---

#### Step 12 - `generate_html_report`

**Type:** PySpark
**Purpose:** Generate comprehensive HTML migration report and prepare for email delivery

- Queries tracking tables for run info and table status
- **Generates HTML report with comprehensive sections:**
  1. **Migration Summary** - Total/successful/failed tables, data volume, file counts, incremental runs
  2. **Validation Summary** - Tables validated, passed/failed counts, mismatch breakdowns
  3. **Table Migration Details** - Per-table status, durations for discovery/DistCp/creation/validation
  4. **Metadata Validation Results** - Row count comparison, partition comparison, schema comparison
  5. **Data Validation Results** - File size comparison (MapR vs S3), file count comparison
  6. **Performance Metrics** - Data volume, DistCp speed (MB/s), rows/second, end-to-end duration
- Writes HTML report to S3 at `{report_location}/{run_id}_report.html`
- **Returns both:**
  - `report_path` - S3 location for audit/archival
  - `html_content` - Full HTML string for direct email delivery

---

#### Step 13 - `send_migration_report_email`

**Type:** PySpark  
**Purpose:** Send HTML migration report via email using SMTP

- Receives HTML content directly from `generate_html_report` task
- Extracts email configuration:
  - SMTP connection ID from Airflow variable
  - Recipients list (comma-separated) from Airflow variable
- Sends email with:
  - Subject: `Migration Report - {run_id}`
  - Body: Full HTML report (no S3 read required)
- **Skips email if:**
  - No recipients configured (`migration_email_recipients` variable empty)
  - Returns `{'sent': False, 'reason': 'no_recipients'}`
- Logs delivery status and recipient list
- Returns result with `sent` status, `recipients`, and `report_path`

---

#### Step 14 - `finalize_run`

**Type:** PySpark  
**Purpose:** Aggregate statistics and mark migration run as complete

- Queries `migration_table_status` to calculate:
  - Total tables processed
  - Successful tables (not in FAILED/PENDING states)
  - Failed tables
- Updates `migration_runs` table with:
  - `status` = COMPLETED
  - `completed_at` = Current timestamp
  - Final counts
- Provides summary metrics for the entire migration run

---

#### Step 15 - `cleanup_edge`

**Type:** SSH  
**Purpose:** Clean up temporary files on MapR edge node

- Removes temporary directory created in `cluster_login_setup`
- Cleans up DistCp log files
- Ensures edge node disk space is freed
- Failures are ignored

---

### Status Progression

```
DISCOVERED
    ↓
COPIED (DistCp successful)
    ↓
TABLE_CREATED (Hive table created/repaired)
    ↓
VALIDATED (All validations passed)

(Any stage can fail → FAILED)
```

---

## DAG 2: Iceberg Migration

### Purpose

Converts existing Hive tables in S3 to Apache Iceberg format using Spark procedures, with comprehensive validation and parent run tracking.

---

### Key Features

- **Two Migration Strategies:**
  - **In-place**: Convert existing Hive table to Iceberg (overwrites metadata)
  - **Snapshot**: Create separate Iceberg table alongside Hive table
- **Parent Run Tracking** - Links back to original MapR-to-S3 migration
- **Comprehensive Validation** - Row counts, partition counts, schema comparison
- **HTML Reporting** - Detailed migration and validation reports

---

### Duration Tracking

Tasks decorated with `@track_duration` automatically capture execution time:

- **Mechanism**: Decorator wraps task function and measures start/end time
- **Storage**: Adds `_task_duration` field to task result dictionary
- **XCom**: Duration flows through task dependencies via XCom
- **Tracking**: Saved to tracking tables in `*_duration_seconds` columns

**Tracked tasks:**

- `migrate_tables_to_iceberg` → `migration_duration_seconds`
- `validate_iceberg_tables` → `validation_duration_seconds`

---

### Migration Strategies

### Inplace Migration

#### What it does

- Converts existing Hive table to Iceberg format
- Uses Spark procedure: CALL spark_catalog.system.migrate('{table}')
- Overwrites table metadata - table becomes Iceberg table
- Original Hive table is lost (irreversible)

---

#### Characteristics

- Database name: Same as source
- Table name: Same as source
- Location: Same as source (metadata changes only)
- Storage: No data duplication
- Queries: Must use Iceberg-compatible engine

---

### Snapshot Migration

#### What it does

- Creates new Iceberg table alongside existing Hive table
- Uses Spark procedure: CALL spark_catalog.system.snapshot('{source}', '{dest}')
- Preserves original Hive table - both tables exist
- Creates separate Iceberg table with snapshot of data

---

### Characteristics

- Database name: Configurable (defaults to {source}\_iceberg)
- Table name: Same as source
- Location: Same as source (metadata layer only)
- Storage: Minimal duplication (metadata only)
- Queries: Can query both Hive and Iceberg versions

---

### Excel Configuration Format

**Required Columns:**

| Column                         | Required | Description                                       | Example                 |
| ------------------------------ | -------- | ------------------------------------------------- | ----------------------- |
| `database`                     | **Yes**  | Source database name                              | `sales_data_s3`         |
| `table`                        | No       | Table pattern (supports `*` wildcards)            | `transactions_*` or `*` |
| `inplace_migration`            | No       | `T`/`True` for in-place, `F`/`False` for snapshot | `F`                     |
| `destination_iceberg_database` | No       | Destination database (defaults based on strategy) | `sales_data_iceberg`    |

---

**Default Behavior:**

- If `inplace_migration = True`: Database remains the same and metadata migrates to Iceberg
- If `inplace_migration = False`: Database defaults to `{source_database}_iceberg` and creates seperate Iceberg metadata table

---

### Task Flow

```
init_iceberg_tracking_tables
    ↓
create_iceberg_migration_run
    ↓
parse_iceberg_excel
    ↓
lookup_parent_migration_run (Links to DAG 1)
    ↓
update_parent_run_id
    ↓
┌───────────────────────────────────────────────┐
│  Dynamic Task Mapping (per database config)   │
│                                               │
│  discover_hive_tables (PySpark)               │
│    ↓                                          │
│  migrate_tables_to_iceberg (PySpark)          │
│    ↓                                          │
│  update_migration_durations                   │
│    ↓                                          │
│  validate_iceberg_tables (PySpark)            │
│    ↓                                          │
│  update_iceberg_validation_status             │
└───────────────────────────────────────────────┘
    ↓
generate_iceberg_html_report
    ↓
send_iceberg_report_email (PySpark: Email report)
    ↓
finalize_iceberg_run
```

---

### Task Summaries

#### Step 0 - `init_iceberg_tracking_tables`

**Type:** PySpark  
**Purpose:** Initialize Iceberg migration tracking infrastructure

- Creates the `migration_tracking` database if it doesn't exist
- Creates two Iceberg tables for tracking:
  - `iceberg_migration_runs` - Run-level metadata (run ID, status, counts, timestamps)
  - `iceberg_migration_table_status` - Table-level tracking
- Ensures tracking tables persist across all iceberg migration runs

---

#### Step 1 - `create_iceberg_migration_run`

**Type:** PySpark  
**Purpose:** Generate unique run identifier and initialize run record

- Creates a unique run ID with timestamp and UUID
- Inserts initial record into `iceberg_migration_runs` table with status `RUNNING`
- Stores DAG configuration snapshot for audit trail
- Returns run ID for use in downstream tasks

---

#### Step 2 - `parse_iceberg_excel`

**Type:** PySpark  
**Purpose:** Read and parse Excel configuration file from S3 for Iceberg migration

- Reads Excel file from S3 using `pyspark.pandas.read_excel`
- Normalizes column names (lowercase, strip whitespace, replace spaces with underscores)
- Validates and defaults configuration values:
  - `destination_iceberg_database` defaults to <database_iceberg>
  - `inplace_migration` defaults to `False`
- Expands to list of database configurations for dynamic task mapping
- Filters out rows with empty database names

---

#### Step 3 - `lookup_parent_migration_run`

**Type:** PySpark
**Purpose:** Find parent MapR-to-S3 migration run ID by querying DAG 1 tracking tables

- Expands table patterns to get concrete table names
- For each table, queries DAG 1 tracking
  1. Finds most recent successful MapR-to-S3 migration for this table
  2. Only considers migrations that reached TABLE_CREATED or COPIED status
  3. Creates mapping: {database.table: parent_run_id}
- Determines most common parent run ID
- Returns lookup result

---

#### Step 4 - `update_parent_run_id`

**Type:** PySpark
**Purpose:** Update iceberg_migration_runs table with parent run link

- Extracts parent run ID from lookup result
- Updates run record if parent found

---

#### Step 5 - `discover_hive_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Discover Hive tables matching pattern in the source database

- Lists all tables in source database
- Filters tables by pattern
- For each matched table, gets location
- Returns discovery result

---

#### Step 6 - `migrate_tables_to_iceberg`

**Type:** PySpark (mapped per database)
**Purpose:** Migrate Hive tables to Iceberg format using Spark procedures

- Creates destination database if needed (snapshot mode only)
- For each discovered table
  - Gets Hive table row count and partition count (baseline)
  - Executes appropriate Spark procedure:
    A. **Inplace Migration:**
    - Converts Hive table to Iceberg in-place
    - Same database and table name
    - Overwrites table metadata (irreversible)
    - Table type changes from Hive external to Iceberg
      B. **Snapshot Migration:**
    - Creates new Iceberg table
    - Different database, same table name
    - Preserves original Hive table
    - Both tables point to same data location
- Gets Iceberg table row count and partition count (validation)
- Validate count and return migration results

---

#### Step 7 - `update_migration_durations`

**Type:** PySpark (mapped per database)  
**Purpose:** Update tracking table with migration durations extracted from XCom

- Extracts migration duration from @track_duration decorator
- Updates all records for this run:

---

#### Step 8 - `validate_iceberg_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Validate Iceberg tables: comprehensive Hive vs Iceberg comparison

- For each table:
  - Check if migration succeeded
  - Get destination row count
  - Get destination partition count
  - Perform schema comparison
  - Perform validation checks

---

#### Step 9 - `update_iceberg_validation_status`

**Type:** PySpark (mapped per database)  
**Purpose:** Update Iceberg tracking with validation results

- For each table in validation results:
  - Skip if validation not completed
  - Escape and truncates error/schema differences
- Update tracking table
- Determine final overall_status

**Final status meanings:**

- COMPLETED: Iceberg migration procedure executed successfully
- VALIDATED: All validations passed (row counts, partition counts, schema) - MIGRATION SUCCESS
- VALIDATION_FAILED: One or more validations failed
- FAILED: Iceberg migration procedure failed

---

#### Step 10 - `generate_iceberg_html_report`

**Type:** PySpark
**Purpose:** Generate comprehensive HTML migration report

- Queries tracking tables
- **Generates HTML report with comprehensive sections:**
  1. **Migration Summary** - Total/successful/failed tables, row counts, incremental runs
  2. **Table Migration Details** - Per-table status, durations for migration/validation
  3. **Validation Results (Hive vs Iceberg)** - Row count comparison, partition comparison, schema comparison
  4. **Performance Metrics** - Rows migrated, Migration speed (MB/s), rows/second, end-to-end duration
- Writes HTML report to S3 at `{report_location}/{run_id}_iceberg_report.html`
- **Returns both:**
  - `report_path` - S3 location for audit/archival
  - `html_content` - Full HTML string for direct email delivery

---

#### Step 11 - `send_iceberg_report_email`

**Type:** PySpark  
**Purpose:** Send HTML Iceberg migration report via email using SMTP

- Receives HTML content directly from `generate_iceberg_html_report` task
- Extracts email configuration:
  - SMTP connection ID from Airflow variable
  - Recipients list (comma-separated) from Airflow variable
- Sends email with:
  - Subject: `Iceberg Migration Report - {run_id}`
  - Body: Full HTML report (no S3 read required)
- **Skips email if:**
  - No recipients configured (`migration_email_recipients` variable empty)
  - Returns `{'sent': False, 'reason': 'no_recipients'}`
- Logs delivery status and recipient list
- Returns result with `sent` status, `recipients`, and `report_path`

---

#### Step 12 - `finalize_iceberg_run`

**Type:** PySpark  
**Purpose:** Aggregate statistics and mark migration run as complete

- Queries `iceberg_migration_table_status` to calculate:
  - Total tables processed
  - Successful tables (not in FAILED/PENDING states)
  - Failed tables
- Updates `iceberg_migration_runs` table with:
  - `status` = COMPLETED
  - `completed_at` = Current timestamp
  - Final counts
- Provides summary metrics for the entire migration run

---

### Status Progression

```
PENDING
    ↓
    │ [Iceberg migration procedure executed]
    ↓
COMPLETED (Iceberg migration successful)
    ↓
    │ [Validation: Hive vs Iceberg comparison]
    │ [All validations pass]
    ↓
VALIDATED
    │
    │ (OR, if any validation fails)
    ↓
VALIDATION_FAILED
```

---

## Tracking Tables

### MapR-to-S3 Migration Tracking

1. **migration_tracking.migration_runs**: Run-level metadata for MapR-to-S3 migrations.
2. **migration_tracking.migration_table_status**: Table-level tracking for MapR-to-S3 migrations.
3. **migration_tracking.validation_results**: Aggregated validation summary per run.

---

### Iceberg Migration Tracking

1. **migration_tracking.iceberg_migration_runs**: Run-level metadata for Iceberg migrations.
2. **migration_tracking.iceberg_migration_table_status**: Table-level tracking for Iceberg migrations.

---

End of Document
