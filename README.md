# MapR to S3 Migration DAG

An automated **Airflow TaskFlow-based migration pipeline** for orchestrating large-scale migration of Hive tables from MapR-FS to Amazon S3, with optional Iceberg format conversion and comprehensive tracking.

---

## Overview

This DAG automates the end-to-end process of migrating Hive tables from a MapR cluster to S3:

- **Metadata Discovery** - Automated table metadata extraction using Beeline
- **Data Transfer** - Efficient bulk data copy using Hadoop DistCp
- **Metadata Creation** - Hive external table recreation in S3
- **Iceberg Conversion** - Optional conversion to Apache Iceberg format
- **Incremental Support** - Resume and update existing migrations with `-update` flag
- **Comprehensive Tracking** - All operations tracked in Iceberg tables

---

## Key Features

- **Parallel Processing** - Dynamic task mapping for concurrent database/table migrations
- **Incremental Migration** - Automatically detects existing data and syncs only changes
- **Partition Support** - Automatic partition discovery and repair (`MSCK REPAIR TABLE`)
- **Format Preservation** - Supports Parquet, ORC, and Avro formats
- **Error Recovery** - Per-table error handling with detailed tracking
- **SSH Operations** - 24-hour timeout for long-running DistCp jobs
- **Validation** - Row count validation for Iceberg conversions

---

## Task Summaries

### Step 0 - `init_tracking_tables`

**Type:** PySpark  
**Purpose:** Initialize the migration tracking infrastructure

- Creates the `migration_tracking` database if it doesn't exist
- Creates two Iceberg tables for tracking:
  - `migration_runs` - Run-level metadata (run ID, status, counts, timestamps)
  - `migration_table_status` - Table-level tracking (discovery, DistCp, creation, Iceberg status)
- Ensures tracking tables persist across all migration runs

---

### Step 1 - `create_migration_run`

**Type:** PySpark  
**Purpose:** Generate unique run identifier and initialize run record

- Creates a unique run ID with timestamp and UUID
- Inserts initial record into `migration_runs` table with status `RUNNING`
- Stores DAG configuration snapshot for audit trail
- Returns run ID for use in downstream tasks

---

### Step 2 - `parse_excel`

**Type:** PySpark  
**Purpose:** Read and parse Excel configuration file from S3

- Reads Excel file from S3 using `pyspark.pandas.read_excel`
- Normalizes column names (lowercase, strip whitespace, replace spaces with underscores)
- Validates and defaults configuration values:
  - `dest_database` defaults to source database name
  - `bucket` defaults to `migration_default_s3_bucket` variable
  - `table` pattern defaults to `*` (all tables)
  - `convert_to_iceberg` defaults to `False`
- Expands to list of database configurations for dynamic task mapping
- Filters out rows with empty database names

**Excel columns:**
| Column | Required | Example |
|--------|----------|---------|
| `database` | Yes | `sales_data` |
| `table` | No | `transactions_*` or `*` |
| `dest database` | No | `sales_data_s3` |
| `bucket` | No | `s3a://data-lake` |
| `convert to iceberg` | No | `T`, `True`, `Yes`, `1` |

---

### Step 3 - `mapr_token_setup`

**Type:** SSH  
**Purpose:** Authenticate to MapR cluster and prepare edge node environment

- Connects to MapR edge node via SSH
- Authenticates using one of three methods:
  1. **Password authentication** - Uses `mapr_user` and `mapr_password` variables
  2. **Kerberos** - Uses existing Kerberos ticket if available
  3. **Existing MapR ticket** - Validates and uses existing valid ticket
- Verifies MapR ticket validity with `maprlogin print`
- Creates temporary working directory on edge node (`/tmp/migration/{run_id}`)
- Ensures all subsequent SSH operations can access MapR filesystem

---

### Step 4 - `discover_tables_ssh`

**Type:** SSH (mapped per database)  
**Purpose:** Discover table metadata from Hive using Beeline

- Executes on edge node via SSH for each database in Excel config
- Uses Beeline to connect to HiveServer2 over JDBC
- Discovers tables matching the pattern (supports `*` wildcards)
- For each table, extracts:
  - **Schema** - Column names and data types
  - **Location** - MapR-FS HDFS path
  - **Format** - Parquet, ORC, or Avro (detected from InputFormat)
  - **Partitions** - Partition spec and count (via `SHOW PARTITIONS`)
  - **Partition columns** - Extracted from table metadata
- Generates JSON output with all discovered metadata
- Determines S3 destination path: `{bucket}/{dest_database}/{table_name}`

---

### Step 5 - `record_discovered_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Persist discovered table metadata in Iceberg tracking table

- Inserts or updates records in `migration_table_status` for each discovered table
- Uses `MERGE` statement to handle both new discoveries and re-runs
- Stores comprehensive metadata: schema JSON, partition list, file format, location
- Sets initial status to `DISCOVERED`
- Enables downstream tasks to access table metadata without re-querying Hive

---

### Step 6 - `run_distcp_ssh`

**Type:** SSH (mapped per database)  
**Purpose:** Copy data from MapR-FS to S3 using Hadoop DistCp

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
- Logs written to `{temp_dir}/distcp_{table}.log`
- **Timeout:** 24 hours per table (configurable via `SSH_COMMAND_TIMEOUT`)

---

### Step 7 - `update_distcp_status`

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

### Step 8 - `create_hive_tables`

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

### Step 9 - `update_table_create_status`

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

### Step 10 - `migrate_to_iceberg`

**Type:** PySpark (mapped per database)  
**Purpose:** Convert Hive external tables to Iceberg format (optional)

- **Only executes if:** `convert_to_iceberg = True` in Excel config
- For each table marked for Iceberg conversion:
  - Checks if Iceberg table already exists (skip if yes)
  - Creates new Iceberg table via CTAS (CREATE TABLE AS SELECT)
  - Preserves partitioning from source table
  - Stores Iceberg table at `{s3_location}_iceberg`
  - **Validation:**
    - Compares row counts between source and Iceberg table
    - Flags mismatch in tracking table
- Skips tables where previous steps failed

---

### Step 11 - `update_iceberg_status`

**Type:** PySpark (mapped per database)  
**Purpose:** Update tracking table with Iceberg migration results

- Updates `migration_table_status` for each table with:
  - `iceberg_status` - COMPLETED, FAILED, or SKIPPED
  - `iceberg_completed_at` - Timestamp
  - `iceberg_table_name` - Fully qualified Iceberg table name
  - `overall_status` - Updated to ICEBERG_MIGRATED or ICEBERG_FAILED
  - `error_message` - Error details if failed
- Records row count validation results
- Provides final status for each table's migration

---

### Step 12 - `finalize_run`

**Type:** PySpark  
**Purpose:** Aggregate statistics and mark migration run as complete

- Queries `migration_table_status` to calculate:
  - Total tables processed
  - Successful tables (not in FAILED/PENDING/ICEBERG_FAILED states)
  - Failed tables
- Updates `migration_runs` table with:
  - `status` = COMPLETED
  - `completed_at` = Current timestamp
  - Final counts
- Provides summary metrics for the entire migration run

---

### Step 13 - `cleanup_edge`

**Type:** SSH  
**Purpose:** Clean up temporary files on MapR edge node

- Removes temporary directory created in `mapr_token_setup`
- Cleans up DistCp log files
- Ensures edge node disk space is freed
- Failures are ignored

---

## Architecture & Task Flow

### Data Flow Diagram

```
┌─────────────────┐
│  Excel Config   │  (S3)
│  (Input)        │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│                    INITIALIZATION                            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │ Init Tracking│ →  │ Create Run   │ →  │ Parse Excel  │  │
│  │ Tables       │    │ Record       │    │ Config       │  │
│  └──────────────┘    └──────────────┘    └──────┬───────┘  │
│                                                    │          │
└───────────────────────────────────────────────────┼──────────┘
                                                     │
                          ┌──────────────────────────┘
                          │
                          ▼
         ┌────────────────────────────┐
         │  MapR Authentication       │
         │  (mapr_token_setup)        │
         └────────────┬───────────────┘
                      │
    ┌─────────────────┴─────────────────┐
    │  Dynamic Task Mapping (per DB)    │
    │  ┌─────────────────────────────┐  │
    │  │  1. discover_tables_ssh     │  │  ← SSH/Beeline
    │  │  2. record_discovered_tables│  │  ← PySpark/Iceberg
    │  │  3. run_distcp_ssh          │  │  ← SSH/DistCp (24h timeout)
    │  │  4. update_distcp_status    │  │  ← PySpark/Iceberg
    │  │  5. create_hive_tables      │  │  ← PySpark/Hive
    │  │  6. update_table_create...  │  │  ← PySpark/Iceberg
    │  │  7. migrate_to_iceberg      │  │  ← PySpark/Iceberg (optional)
    │  │  8. update_iceberg_status   │  │  ← PySpark/Iceberg
    │  └─────────────────────────────┘  │
    └─────────────────┬─────────────────┘
                      │
                      ▼
         ┌────────────────────────────┐
         │  finalize_run              │  ← Aggregate stats
         └────────────┬───────────────┘
                      │
                      ▼
         ┌────────────────────────────┐
         │  cleanup_edge              │  ← Remove temp files
         └────────────────────────────┘
```

### Migration Pipeline Stages

```
MapR-FS (Hive Tables)
         │
         │ [DistCp]
         ▼
   S3 (Raw Data Files)
         │
         │ [Hive External Table DDL]
         ▼
   S3 (Queryable via Hive/Trino)
         │
         │ [CTAS - Optional]
         ▼
   S3 (Iceberg Format)
```

### Status Flow

**Overall Status Progression:**

```
DISCOVERED
    ↓
COPIED
    ↓
TABLE_CREATED
    ↓
ICEBERG_MIGRATED  (if convert_to_iceberg=True)

   (Any stage can transition to)
    ↓
FAILED / ICEBERG_FAILED
```

**Individual Stage Statuses:**

- `COMPLETED` - Stage finished successfully
- `FAILED` - Stage failed with error
- `SKIPPED` - Stage skipped due to previous failure or configuration
- `PENDING` - Stage not yet started

---
